#pragma once

#include "DB_index.hh"
#include "lib.h"
#include "kv.h"
#include "wh.h"

namespace bench
{
    class WhRef
    {
    public:
        wormref *ref;
        WhRef(wormhole *wh)
        {
            this->ref = wh_ref(wh);
        }

        ~WhRef()
        {
            wh_unref(this->ref);
        }
    };

    template <typename K, typename V, typename DBParams>
    class ordered_index : public TObject
    {
    public:
        typedef K key_type;
        typedef V value_type;
        typedef commutators::Commutator<value_type> comm_type;

        typedef typename get_version<DBParams>::type version_type;
        using accessor_t = typename index_common<K, V, DBParams>::accessor_t;
        static constexpr typename version_type::type invalid_bit = TransactionTid::user_bit;
        static constexpr TransItem::flags_type insert_bit = TransItem::user0_bit;
        static constexpr TransItem::flags_type delete_bit = TransItem::user0_bit << 1u;
        static constexpr TransItem::flags_type row_update_bit = TransItem::user0_bit << 2u;
        static constexpr TransItem::flags_type row_cell_bit = TransItem::user0_bit << 3u;

        typedef typename value_type::NamedColumn NamedColumn;
        typedef IndexValueContainer<V, version_type> value_container_type;

        static constexpr bool value_is_small = is_small<V>::value;

        static constexpr bool index_read_my_write = DBParams::RdMyWr;

        // **** Return Types ****
        typedef std::tuple<bool, bool, uintptr_t, const value_type *> sel_return_type;
        typedef std::tuple<bool, bool> ins_return_type;
        typedef std::tuple<bool, bool> del_return_type;
        typedef std::tuple<bool, bool, uintptr_t, UniRecordAccessor<V>> sel_split_return_type;

        typedef wormhole *table_type;

        struct internal_elem
        {
            key_type key;
            value_container_type row_container;
            bool deleted;

            internal_elem(const key_type &k, const value_type &v, bool valid)
                : key(k),
                  row_container((valid ? Sto::initialized_tid() : (Sto::initialized_tid() | invalid_bit)),
                                !valid, v),
                  deleted(false) {}

            version_type &version()
            {
                return row_container.row_version();
            }

            bool valid()
            {
                return !(version().value() & invalid_bit);
            }
        };

        using column_access_t = typename split_version_helpers<ordered_index<K, V, DBParams>>::column_access_t;
        using item_key_t = typename split_version_helpers<ordered_index<K, V, DBParams>>::item_key_t;
        template <typename T>
        static constexpr auto column_to_cell_accesses = split_version_helpers<ordered_index<K, V, DBParams>>::template column_to_cell_accesses<T>;
        template <typename T>
        static constexpr auto extract_item_list = split_version_helpers<ordered_index<K, V, DBParams>>::template extract_item_list<T>;

        ordered_index()
        {
            this->table_ = wh_create();
        }

        ~ordered_index()
        {
            wh_destroy(this->table_);
        }
        static void thread_init()
        {
        }

        value_type *nontrans_get(const key_type &k)
        {
            internal_elem *e;
            WhRef table(this->table_);
            uint32_t key_len;
            bool found = wh_get(table.ref, &k, sizeof(k), &e, sizeof(e), &key_len);

            if (found)
            {
                return &(e->row_container.row);
            }
            else
                return nullptr;
        }

        void nontrans_put(const key_type &k, const value_type &v)
        {
            internal_elem *e;
            WhRef table(this->table_);
            uint32_t key_len;
            bool found = wh_get(table.ref, &k, sizeof(k), &e, sizeof(e), &key_len);
            if (found)
            {
                if (value_is_small)
                    e->row_container.row = v;
                else
                    copy_row(e, &v);
            }
            else
            {
                e = new internal_elem(k, v, true);
                wh_put(table.ref, &k, sizeof(k), &e, sizeof(e));
            }
        }

        ins_return_type insert_row(const key_type &key, value_type *vptr, bool overwrite = false)
        {
            WhRef table(this->table_);
            internal_elem *e;
            uint32_t len_out;
            bool found = wh_get(table.ref, &key, sizeof(key), &e, sizeof(e), &len_out);
            if (found)
            {
                TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

                if (is_phantom(e, row_item))
                    goto abort;

                if (index_read_my_write)
                {
                    if (has_delete(row_item))
                    {
                        auto proxy = row_item.clear_flags(delete_bit).clear_write();

                        if (value_is_small)
                            proxy.add_write(*vptr);
                        else
                            proxy.add_write(vptr);

                        return ins_return_type(true, false);
                    }
                }

                if (overwrite)
                {
                    bool ok;
                    if (value_is_small)
                        ok = version_adapter::select_for_overwrite(row_item, e->version(), *vptr);
                    else
                        ok = version_adapter::select_for_overwrite(row_item, e->version(), vptr);
                    if (!ok)
                        goto abort;
                    if (index_read_my_write)
                    {
                        if (has_insert(row_item))
                        {
                            copy_row(e, vptr);
                        }
                    }
                }
                else
                {
                    // observes that the row exists, but nothing more
                    if (!row_item.observe(e->version()))
                        goto abort;
                }
            }
            else
            {
                e = new internal_elem(key, vptr ? *vptr : value_type(),
                                      false /*!valid*/);

                // TODO: atomic find and insert
                wh_put(table.ref, &key, sizeof(key), &e, sizeof(e));
                // TODO: Update internode version

                TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));
                row_item.acquire_write(e->version());
                row_item.add_flags(insert_bit);
            }

            return ins_return_type(true, found);

        abort:
            return ins_return_type(false, false);
        }

        void update_row(uintptr_t rid, value_type *new_row)
        {
            auto e = reinterpret_cast<internal_elem *>(rid);
            auto row_item = Sto::item(this, item_key_t::row_item_key(e));
            if (value_is_small)
            {
                row_item.acquire_write(e->version(), *new_row);
            }
            else
            {
                row_item.acquire_write(e->version(), new_row);
            }
        }

        void update_row(uintptr_t rid, const comm_type &comm)
        {
            assert(&comm);
            auto row_item = Sto::item(this, item_key_t::row_item_key(reinterpret_cast<internal_elem *>(rid)));
            row_item.add_commute(comm);
        }

        // **** select row functions ****
        sel_split_return_type
        select_split_row(const key_type &key, std::initializer_list<column_access_t> accesses)
        {

            internal_elem *e;
            WhRef table(this->table_);
            uint32_t key_len;

            bool found = wh_get(table.ref, &key, sizeof(key), &e, sizeof(e), &key_len);
            if (found)
            {
                return select_split_row(reinterpret_cast<uintptr_t>(e), accesses);
            }
            return {
                // TODO: register version when not found!
                true,
                false,
                0,
                UniRecordAccessor<V>(nullptr)};
        }

        sel_split_return_type
        select_split_row(uintptr_t rid, std::initializer_list<column_access_t> accesses)
        {
            auto e = reinterpret_cast<internal_elem *>(rid);
            TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

            // Translate from column accesses to cell accesses
            // all buffered writes are only stored in the wdata_ of the row item (to avoid redundant copies)
            auto cell_accesses = column_to_cell_accesses<value_container_type>(accesses);

            std::array<TransItem *, value_container_type::num_versions> cell_items{};
            bool any_has_write;
            bool ok;
            std::tie(any_has_write, cell_items) = extract_item_list<value_container_type>(cell_accesses, this, e);

            if (is_phantom(e, row_item))
                goto abort;

            if (index_read_my_write)
            {
                if (has_delete(row_item))
                {
                    return {true, false, 0, UniRecordAccessor<V>(nullptr)};
                }
                if (any_has_write || has_row_update(row_item))
                {
                    value_type *vptr;
                    if (has_insert(row_item))
                        vptr = &e->row_container.row;
                    else
                        vptr = row_item.template raw_write_value<value_type *>();
                    return {true, true, rid, UniRecordAccessor<V>(vptr)};
                }
            }

            ok = access_all(cell_accesses, cell_items, e->row_container);
            if (!ok)
                goto abort;

            return {true, true, rid, UniRecordAccessor<V>(&(e->row_container.row))};

        abort:
            return {false, false, 0, UniRecordAccessor<V>(nullptr)};
        }

        del_return_type
        delete_row(const key_type &key)
        {
            internal_elem *e;
            WhRef table(this->table_);
            uint32_t key_len;

            bool found = wh_get(table.ref, &key, sizeof(key), &e, sizeof(e), &key_len);
            if (found)
            {
                TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));

                if (is_phantom(e, row_item))
                {
                    goto abort;
                }

                if (index_read_my_write)
                {
                    if (has_delete(row_item))
                        return del_return_type(true, false);
                    if (!e->valid() && has_insert(row_item))
                    {
                        row_item.add_flags(delete_bit);
                        return del_return_type(true, true);
                    }
                }

                // TODO: handle TicToc ttnv
                // Register a TicToc write to the leaf node when necessary.
                // ttnv_register_node_write(lp.node());

                // select_for_update will register an observation and set the write bit of
                // the TItem
                if (!version_adapter::select_for_update(row_item, e->version()))
                {
                    goto abort;
                }
                fence();
                if (e->deleted)
                {
                    goto abort;
                }
                row_item.add_flags(delete_bit);
            }
            else
            {
                // TODO: handle internode version
                /*if (!register_internode_version(lp.node(), lp))
                {
                    goto abort;
                } */
            }

            return del_return_type(true, found);

        abort:
            return del_return_type(false, false);
        }

        template <typename Callback, bool Reverse>
        bool range_scan(const key_type &begin, const key_type &end, Callback callback,
                        std::initializer_list<column_access_t> accesses, bool phantom_protection = true, int limit = -1)
        {
            static_assert(!Reverse); // Wormhole does not support reverse
            assert((limit == -1) || (limit > 0));
            /*auto node_callback = [&](leaf_type *node,
                                     typename unlocked_cursor_type::nodeversion_value_type version)
            {
                // TODO: implement scan_track_node_version: return ((!phantom_protection) || scan_track_node_version(node, version));
                return true;
            };*/

            auto cell_accesses = column_to_cell_accesses<value_container_type>(accesses);

            auto value_callback = [&](const key_type &key, internal_elem *e, bool &ret, bool &count)
            {
                TransProxy row_item = index_read_my_write ? Sto::item(this, item_key_t::row_item_key(e))
                                                          : Sto::fresh_item(this, item_key_t::row_item_key(e));

                bool any_has_write;
                std::array<TransItem *, value_container_type::num_versions> cell_items{};
                std::tie(any_has_write, cell_items) = extract_item_list<value_container_type>(cell_accesses, this, e);

                if (index_read_my_write)
                {
                    if (has_delete(row_item))
                    {
                        ret = true;
                        count = false;
                        return true;
                    }
                    if (any_has_write)
                    {
                        if (has_insert(row_item))
                            ret = callback(key, &(e->row_container.row));
                        else
                            ret = callback(key, row_item.template raw_write_value<value_type *>());
                        return true;
                    }
                }

                bool ok = access_all(cell_accesses, cell_items, e->row_container);
                if (!ok)
                    return false;
                // bool ok = item.observe(e->version);
                // if (Adaptive) {
                //     ok = item.observe(e->version, true/*force occ*/);
                // } else {
                //     ok = item.observe(e->version);
                // }

                // skip invalid (inserted but yet committed) values, but do not abort
                if (!e->valid())
                {
                    ret = true;
                    count = false;
                    return true;
                }

                ret = callback(key_type(key), &(e->row_container.row));
                return true;
            };

            WhRef table(this->table_);
            internal_elem *e;
            uint32_t len_out;
            key_type key;
            uint32_t len_key_out;

            wormhole_iter *iter = wh_iter_create(table.ref);
            wh_iter_seek(iter, &begin, sizeof(key_type));
            int scan_count = 0;
            while (wh_iter_valid(iter))
            {
                wh_iter_peek(iter, &key, sizeof(key), &len_key_out, &e, sizeof(e), &len_out); // TODO: check return value??
                wh_iter_park(iter);
                bool visited = false;
                bool count = true;
                if (!value_callback(key, e, visited, count))
                {
                    return false;
                }
                else
                {
                    if (!visited)
                    {
                        return false;
                    }
                    if (count)
                    {
                        scan_count++;
                    }
                    if (limit > 0 && scan_count >= limit)
                    {
                        return true;
                    }
                }
                wh_iter_seek(iter, NULL, NULL);
                wh_iter_skip1(iter);
            }
            wh_iter_destroy(iter);
            return true;
        }
        template <typename Callback, bool Reverse>
        bool range_scan(const key_type &begin, const key_type &end, Callback callback,
                        RowAccess access, bool phantom_protection = true, int limit = -1)
        {
            static_assert(!Reverse); // Wormhole does not support reverse
            assert((limit == -1) || (limit > 0));
            /* auto node_callback = [&](leaf_type *node,
                                      typename unlocked_cursor_type::nodeversion_value_type version)
             {
                 return ((!phantom_protection) || scan_track_node_version(node, version));
             };*/

            auto value_callback = [&](const lcdf::Str &key, internal_elem *e, bool &ret, bool &count)
            {
                TransProxy row_item = index_read_my_write ? Sto::item(this, item_key_t::row_item_key(e))
                                                          : Sto::fresh_item(this, item_key_t::row_item_key(e));

                if (index_read_my_write)
                {
                    if (has_delete(row_item))
                    {
                        ret = true;
                        count = false;
                        return true;
                    }
                    if (has_row_update(row_item))
                    {
                        if (has_insert(row_item))
                            ret = callback(key_type(key), &(e->row_container.row));
                        else
                            ret = callback(key_type(key), row_item.template raw_write_value<value_type *>());
                        return true;
                    }
                }

                bool ok = true;
                switch (access)
                {
                case RowAccess::ObserveValue:
                case RowAccess::ObserveExists:
                    ok = row_item.observe(e->version());
                    break;
                case RowAccess::None:
                    break;
                default:
                    always_assert(false, "unsupported access type in range_scan");
                    break;
                }

                if (!ok)
                    return false;

                // skip invalid (inserted but yet committed) values, but do not abort
                if (!e->valid())
                {
                    ret = true;
                    count = false;
                    return true;
                }

                ret = callback(key_type(key), &(e->row_container.row));
                return true;
            };

            WhRef table(this->table_);
            internal_elem *e;
            uint32_t len_out;
            key_type key;
            uint32_t len_key_out;

            wormhole_iter *iter = wh_iter_create(table.ref);
            wh_iter_seek(iter, &begin, sizeof(key_type));
            int scan_count = 0;
            while (wh_iter_valid(iter))
            {
                wh_iter_peek(iter, &key, sizeof(key), &len_key_out, &e, sizeof(e), &len_out); // TODO: check return value??
                wh_iter_park(iter);
                bool visited = false;
                bool count = true;
                if (!value_callback(key, e, visited, count))
                {
                    return false;
                }
                else
                {
                    if (!visited)
                    {
                        return false;
                    }
                    if (count)
                    {
                        scan_count++;
                    }
                    if (limit > 0 && scan_count >= limit)
                    {
                        return true;
                    }
                }
                wh_iter_seek(iter, NULL, NULL);
                wh_iter_skip1(iter);
            }
            wh_iter_destroy(iter);
            return true;
        }

        // TObject interface methods
        bool lock(TransItem &item, Transaction &txn) override
        {
            // TODO: assert(!is_internode(item));
            // TODO: handle ttnv
            /* if constexpr (table_params::track_nodes)
            {
                if (is_ttnv(item))
                {
                    auto n = get_internode_address(item);
                    return txn.try_lock(item, *static_cast<leaf_type *>(n)->get_aux_tracker());
                }
            } */
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            if (key.is_row_item())
                return txn.try_lock(item, e->version());
            else
                return txn.try_lock(item, e->row_container.version_at(key.cell_num()));
        }

        bool check(TransItem &item, Transaction &txn) override
        {
            // TODO: handle internode items
            // TODO: handle ttnv TicToc

            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            if (key.is_row_item())
                return e->version().cp_check_version(txn, item);
            else
                return e->row_container.version_at(key.cell_num()).cp_check_version(txn, item);
        }

        void install(TransItem &item, Transaction &txn) override
        {
            // TODO: handle ttnv assert(!is_internode(item));

            /*if constexpr (table_params::track_nodes)
            {
                if (is_ttnv(item))
                {
                    auto n = get_internode_address(item);
                    txn.set_version_unlock(*static_cast<leaf_type *>(n)->get_aux_tracker(), item);
                    return;
                }
            }*/

            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();

            if (key.is_row_item())
            {
                // assert(e->version.is_locked());
                if (has_delete(item))
                {
                    assert(e->valid() && !e->deleted);
                    e->deleted = true;
                    txn.set_version(e->version());
                    return;
                }

                if (!has_insert(item))
                {
                    if (item.has_commute())
                    {
                        comm_type &comm = item.write_value<comm_type>();
                        if (has_row_update(item))
                        {
                            copy_row(e, comm);
                        }
                        else if (has_row_cell(item))
                        {
                            e->row_container.install_cell(comm);
                        }
                    }
                    else
                    {
                        value_type *vptr;
                        if (value_is_small)
                        {
                            vptr = &(item.write_value<value_type>());
                        }
                        else
                        {
                            vptr = item.write_value<value_type *>();
                        }

                        if (has_row_update(item))
                        {
                            if (value_is_small)
                            {
                                e->row_container.row = *vptr;
                            }
                            else
                            {
                                copy_row(e, vptr);
                            }
                        }
                        else if (has_row_cell(item))
                        {
                            // install only the difference part
                            // not sure if works when there are more than 1 minor version fields
                            // should still work
                            e->row_container.install_cell(0, vptr);
                        }
                    }
                }
                txn.set_version_unlock(e->version(), item);
            }
            else
            {
                // skip installation if row-level update is present
                auto row_item = Sto::item(this, item_key_t::row_item_key(e));
                if (!has_row_update(row_item))
                {
                    if (row_item.has_commute())
                    {
                        comm_type &comm = row_item.template write_value<comm_type>();
                        assert(&comm);
                        e->row_container.install_cell(comm);
                    }
                    else
                    {
                        value_type *vptr;
                        if (value_is_small)
                            vptr = &(row_item.template raw_write_value<value_type>());
                        else
                            vptr = row_item.template raw_write_value<value_type *>();

                        e->row_container.install_cell(key.cell_num(), vptr);
                    }
                }

                txn.set_version_unlock(e->row_container.version_at(key.cell_num()), item);
            }
        }

        void unlock(TransItem &item) override
        {
            // TODO: handle internode assert(!is_internode(item));
            /*if constexpr (table_params::track_nodes)
            {
                if (is_ttnv(item))
                {
                    auto n = get_internode_address(item);
                    static_cast<leaf_type *>(n)->get_aux_tracker()->cp_unlock(item);
                    return;
                }
            }*/
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            if (key.is_row_item())
                e->version().cp_unlock(item);
            else
                e->row_container.version_at(key.cell_num()).cp_unlock(item);
        }

        void cleanup(TransItem &item, bool committed) override
        {
            if (committed ? has_delete(item) : has_insert(item))
            {
                auto key = item.key<item_key_t>();
                assert(key.is_row_item());
                internal_elem *e = key.internal_elem_ptr();
                bool ok = _remove(e->key);
                if (!ok)
                {
                    std::cout << "committed=" << committed << ", "
                              << "has_delete=" << has_delete(item) << ", "
                              << "has_insert=" << has_insert(item) << ", "
                              << "locked_at_commit=" << item.locked_at_commit() << std::endl;
                    always_assert(false, "insert-bit exclusive ownership violated");
                }
                item.clear_needs_unlock();
            }
        }
        uint64_t gen_key()
        {
            return fetch_and_add(&key_gen_, 1);
        }

    private:
        uint64_t key_gen_;

        table_type table_;

        static bool
        access_all(std::array<access_t, value_container_type::num_versions> &cell_accesses, std::array<TransItem *, value_container_type::num_versions> &cell_items, value_container_type &row_container)
        {
            for (size_t idx = 0; idx < cell_accesses.size(); ++idx)
            {
                auto &access = cell_accesses[idx];
                auto proxy = TransProxy(*Sto::transaction(), *cell_items[idx]);
                if (static_cast<uint8_t>(access) & static_cast<uint8_t>(access_t::read))
                {
                    if (!proxy.observe(row_container.version_at(idx)))
                        return false;
                }
                if (static_cast<uint8_t>(access) & static_cast<uint8_t>(access_t::write))
                {
                    if (!proxy.acquire_write(row_container.version_at(idx)))
                        return false;
                    if (proxy.item().key<item_key_t>().is_row_item())
                    {
                        proxy.item().add_flags(row_cell_bit);
                    }
                }
            }
            return true;
        }

        bool _remove(const key_type &key)
        {
            // TODO: Think if it needs to lock
            internal_elem *e;
            WhRef table(this->table_);
            uint32_t key_len;

            bool found = wh_get(table.ref, &key, sizeof(key), &e, sizeof(e), &key_len);
            if (found)
            {
                wh_del(table.ref, &key, sizeof(key));
                Transaction::rcu_delete(e);
            }
            return found;
        }

        static bool has_insert(const TransItem &item)
        {
            return (item.flags() & insert_bit) != 0;
        }
        static bool has_delete(const TransItem &item)
        {
            return (item.flags() & delete_bit) != 0;
        }
        static bool has_row_update(const TransItem &item)
        {
            return (item.flags() & row_update_bit) != 0;
        }
        static bool has_row_cell(const TransItem &item)
        {
            return (item.flags() & row_cell_bit) != 0;
        }
        static bool is_phantom(internal_elem *e, const TransItem &item)
        {
            return (!e->valid() && !has_insert(item));
        }

        static void copy_row(internal_elem *e, comm_type &comm)
        {
            comm.operate(e->row_container.row);
        }
        static void copy_row(internal_elem *e, const value_type *new_row)
        {
            if (new_row == nullptr)
                return;
            e->row_container.row = *new_row;
        }
    };
}
