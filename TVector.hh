#pragma once
#include "TWrapped.hh"
#include "TArrayProxy.hh"
#include "TIntPredicate.hh"

template <typename T, template <typename> typename W = TOpaqueWrapped>
class TVector : public TObject {
public:
    using size_type = int;
    using difference_type = int;
    typedef typename W<T>::version_type version_type;
private:
    static constexpr size_type default_capacity = 128;
    using pred_type = TIntRange<size_type>;
    using key_type = int;
    static constexpr key_type size_key = -1;
    /* All information about the TVector's size is stored under size_key.
       If size_key exists (which it almost always does), then:
       * Its predicate_value is a pred_type recording constraints on the size.
       * Its xwrite_value is a pred_type. The `first` component is the initial
         size of the vector. The `second` component is the final size of the
         vector.
       Thus, if xwrite_value.first < xwrite_value.second, the vector grew.
       If xwrite_value.first > xwrite_value.second, the vector shrank.
       Note that the xwrite_value exists even if !has_write(). */

    TransProxy index_item(size_type idx) const {
        TransProxy sitem = size_item();
        pred_type& pval = sitem.predicate_value<pred_type>();
        pred_type& wval = sitem.xwrite_value<pred_type>();
        if (idx >= wval.second)
            version_type::opaque_throw(std::out_of_range("TVector[]"));
        TransProxy item = Sto::item(this, idx);
        if (item.has_write() && !item.has_flag(indexed_bit))
            // observing a pushed-back thing
            pval.observe(wval.first);
        else if (idx < wval.first)
            pval.observe_gt(idx);
        return item.add_flags(indexed_bit);
    }

    static constexpr TransItem::flags_type indexed_bit = TransItem::user0_bit;
public:
    class iterator;
    class const_iterator;
    using size_proxy = TIntRangeProxy<size_type>;
    using difference_proxy = TIntRangeDifferenceProxy<size_type>;
    typedef T value_type;
    typedef typename W<T>::read_type get_type;
    typedef TConstArrayProxy<TVector<T, W> > const_proxy_type;
    typedef TArrayProxy<TVector<T, W> > proxy_type;

    TVector()
        : size_(0), max_size_(0), capacity_(default_capacity) {
        data_ = reinterpret_cast<elem*>(new char[sizeof(elem) * capacity_]);
        for (size_type i = 0; i != capacity_; ++i)
            data_[i].vers = 0;
    }
    ~TVector() {
        using WT = W<T>;
        for (size_type i = 0; i < max_size_; ++i)
            data_[i].v.~WT();
        delete[] reinterpret_cast<char*>(data_);
    }

    size_proxy size() const {
        auto sitem = size_item();
        auto& sinfo = size_info(sitem);
        return size_proxy(&size_predicate(sitem), sinfo.first, sinfo.second - sinfo.first);
    }
    bool empty() const {
        return size() != 0;
    }

    const_proxy_type operator[](size_type i) const {
        return const_proxy_type(this, i);
    }
    proxy_type operator[](size_type i) {
        return proxy_type(this, i);
    }
    const_proxy_type front() const {
        return const_proxy_type(this, 0);
    }
    proxy_type front() {
        return proxy_type(this, 0);
    }
    const_proxy_type back() const {
        auto sitem = size_item();
        auto& sinfo = size_info(sitem);
        size_predicate(sitem).observe(sinfo.first);
        if (!sinfo.second)
            version_type::opaque_throw(std::out_of_range("TVector::back"));
        return const_proxy_type(this, sinfo.second - 1);
    }
    proxy_type back() {
        auto sitem = size_item();
        auto& sinfo = size_info(sitem);
        size_predicate(sitem).observe(sinfo.first);
        if (!sinfo.second)
            version_type::opaque_throw(std::out_of_range("TVector::back"));
        return proxy_type(this, sinfo.second - 1);
    }

    inline iterator begin();
    inline iterator end();
    inline const_iterator cbegin() const;
    inline const_iterator cend() const;
    inline const_iterator begin() const;
    inline const_iterator end() const;

    void push_back(T x) {
        auto sitem = size_item();
        pred_type& wval = sitem.template xwrite_value<pred_type>();
        size_type new_size = wval.second + 1;
        sitem.add_write(pred_type{wval.first, new_size});
        Sto::item(this, new_size - 1).add_write(std::move(x));
    }
    void pop_back() {
        auto sitem = size_item();
        pred_type& wval = sitem.template xwrite_value<pred_type>();
        size_type new_size = wval.second - 1;
        sitem.add_write(pred_type{wval.first, new_size});
        if (new_size < wval.first)
            size_predicate(sitem).observe_ge(wval.first - new_size);
    }

    void clear();
    iterator erase(iterator pos);
    iterator insert(iterator pos, T x);
    void resize(size_type size, T x = T());

    void nontrans_reserve(size_type size);
    void nontrans_push_back(T x) {
        size_type& sz = size_.access();
        assert(sz < capacity_);
        if (sz == max_size_) {
            new(reinterpret_cast<void*>(&data_[sz].v)) W<T>(std::move(x));
            ++max_size_;
        } else
            data_[sz].v.write(std::move(x));
        ++sz;
    }

    // transGet and friends
    get_type transGet(size_type i) const {
        auto item = index_item(i);
        if (item.has_write())
            return item.template write_value<T>();
        else
            return data_[i].v.read(item, data_[i].vers);
    }
    void transPut(size_type i, T x) {
        auto item = index_item(i);
        item.add_write(std::move(x));
    }

    size_type nontrans_size() const {
        return size_.access();
    }
    get_type nontrans_get(size_type i) const {
        assert(i < size_.access());
        return data_[i].v.access();
    }
    void nontrans_put(size_type i, const T& x) {
        assert(i < size_.access());
        data_[i].v.access() = x;
    }
    void nontrans_put(size_type i, T&& x) {
        assert(i < size_.access());
        data_[i].v.access() = std::move(x);
    }

    // transactional methods
    bool check_predicate(TransItem& item, Transaction& txn, bool committing) {
        TransProxy p(txn, item);
        pred_type pred = item.template predicate_value<pred_type>();
        size_type value = size_.wait_snapshot(p, size_vers_, committing);
        return pred.verify(value);
    }
    bool lock(TransItem& item, Transaction& txn) {
        auto key = item.template key<key_type>();
        if (key == size_key)
            return txn.try_lock(item, size_vers_);
        else if (item.has_flag(indexed_bit))
            return txn.try_lock(item, data_[key].vers);
        else {
            assert(size_vers_.is_locked_here(txn));
            return true;
        }
    }
    bool check(const TransItem& item, const Transaction&) {
        auto key = item.template key<key_type>();
        if (key == size_key)
            return item.check_version(size_vers_);
        else {
            assert(item.has_flag(indexed_bit));
            return item.check_version(data_[key].vers);
        }
    }
    void install(TransItem& item, const Transaction& txn) {
        auto key = item.template key<key_type>();
        if (key == size_key) {
            pred_type& wval = item.template xwrite_value<pred_type>();
            original_size_ = size_.access();
            expected_size_ = wval.first;
            size_.write(original_size_ + wval.second - expected_size_);
            txn.set_version(size_vers_);
            return;
        }
        if (size_vers_.is_locked_here(txn)) {
            // maybe we have popped past this point
            if (!item.has_flag(indexed_bit))
                key = original_size_ - (expected_size_ - key);
            if (key >= size_.access()) {
                item.clear_needs_unlock_if_set();
                return;
            }
        }
        assert(key <= max_size_ && key < capacity_);
        if (key == max_size_) {
            new(reinterpret_cast<void*>(&data_[key].v)) W<T>(std::move(item.write_value<T>()));
            ++max_size_;
        } else
            data_[key].v.write(std::move(item.write_value<T>()));
        txn.assign_version_unlock(data_[key].vers, item);
    }
    void unlock(TransItem& item) {
        auto key = item.template key<key_type>();
        if (key == size_key)
            size_vers_.unlock();
        else if (item.has_flag(indexed_bit))
            data_[key].vers.unlock();
    }
    void print(std::ostream& w, const TransItem& item) const {
        w << "{TVector<" << typeid(T).name() << "> " << (void*) this;
        key_type key = item.key<key_type>();
        if (key == size_key) {
            w << ".size @" << item.xwrite_value<pred_type>().second;
            if (item.has_read())
                w << " R" << item.read_value<version_type>();
            else if (item.has_predicate())
                w << ' ' << item.predicate_value<pred_type>();
            if (item.has_write())
                w << " =" << item.xwrite_value<pred_type>().second;
        } else {
            w << "[" << key << "]";
            if (item.has_read())
                w << " R" << item.read_value<version_type>();
            if (item.has_write())
                w << " =" << item.write_value<T>();
        }
        w << "}";
    }
    bool check_not_locked_here(int here) const {
        if (size_vers_.is_locked_here(here))
            return false;
        size_type max_size = max_size_;
        for (size_type i = 0; i != max_size; ++i)
            if (data_[i].vers.is_locked_here(here))
                return false;
        return true;
    }
    void print(std::ostream& w) const;

private:
    struct elem {
        version_type vers;
        W<T> v;
    };
    elem* data_;
    W<size_type> size_;
    version_type size_vers_;
    size_type original_size_; // protected by size_vers_ lock
    size_type expected_size_;
    size_type max_size_; // protected by size_vers_ lock
    size_type capacity_;

    // size helpers
    TransProxy size_item() const {
        auto item = Sto::item(this, size_key);
        if (!item.has_predicate()) {
            item.set_predicate(pred_type::unconstrained());
            size_type sz = size_.snapshot(item, size_vers_);
            item.template xwrite_value<pred_type>() = pred_type{sz, sz};
        }
        return item;
    }
    static pred_type& size_predicate(TransProxy sitem) {
        return sitem.template predicate_value<pred_type>();
    }
    static pred_type& size_predicate(TransItem* sitem) {
        return sitem->template predicate_value<pred_type>();
    }
    static size_type original_size(const TransItem* sitem) {
        return sitem->template xwrite_value<pred_type>().first;
    }
    pred_type& size_predicate() const {
        return size_predicate(size_item());
    }
    static pred_type& size_info(TransProxy sitem) {
        return sitem.template xwrite_value<pred_type>();
    }
    pred_type& size_info() const {
        return size_info(size_item());
    }

    friend class iterator;
    friend class const_iterator;
};


template <typename T, template <typename> typename W>
class TVector<T, W>::const_iterator : public std::iterator<std::random_access_iterator_tag, T> {
public:
    typedef TVector<T, W> vector_type;
    typedef typename vector_type::size_type size_type;
    typedef typename vector_type::difference_type difference_type;
    typedef typename vector_type::pred_type pred_type;
    typedef typename vector_type::difference_proxy difference_proxy;

    const_iterator()
        : a_() {
    }
    const_iterator(const TVector<T, W>* a, size_type i, TransItem* eitem)
        : a_(const_cast<vector_type*>(a)), i_(i), eitem_(eitem) {
    }

    typename vector_type::const_proxy_type operator*() const {
        return vector_type::const_proxy_type(a_, i_);
    }

    bool operator==(const const_iterator& x) const {
        if (a_ != x.a_)
            return false;
        if (different_end(x)) {
            difference_type d = eitem_ ? x.i_ - i_ : i_ - x.i_;
            TransItem* eitem = eitem_ ? eitem_ : x.eitem_;
            size_type sz = a_->original_size(eitem);
            a_->size_predicate(eitem).observe_test_eq(sz, d + sz);
        }
        return i_ == x.i_;
    }
    bool operator!=(const const_iterator& x) const {
        return !(*this == x);
    }
    bool operator<(const const_iterator& x) const {
        assert(a_ == x.a_);
        if (eitem_ && !x.eitem_) {
            size_type sz = a_->original_size(eitem_);
            a_->size_predicate(eitem_).observe_lt(x.i_ - i_ + sz, i_ < x.i_);
        } else if (x.eitem_ && !eitem_) {
            size_type sz = a_->original_size(x.eitem_);
            a_->size_predicate(x.eitem_).observe_gt(i_ - x.i_ + sz, i_ < x.i_);
        }
        return i_ < x.i_;
    }
    bool operator<=(const const_iterator& x) const {
        assert(a_ == x.a_);
        if (eitem_ && !x.eitem_) {
            size_type sz = a_->original_size(eitem_);
            a_->size_predicate(eitem_).observe_le(x.i_ - i_ + sz, i_ <= x.i_);
        } else if (x.eitem_ && !eitem_) {
            size_type sz = a_->original_size(x.eitem_);
            a_->size_predicate(x.eitem_).observe_ge(i_ - x.i_ + sz, i_ <= x.i_);
        }
        return i_ <= x.i_;
    }
    bool operator>(const const_iterator& x) const {
        return x < *this;
    }
    bool operator>=(const const_iterator& x) const {
        return x <= *this;
    }

    const_iterator& operator+=(difference_type delta) {
        i_ += delta;
        return *this;
    }
    const_iterator& operator-=(difference_type delta) {
        i_ += delta;
        return *this;
    }
    const_iterator operator+(difference_type delta) const {
        return const_iterator(a_, i_ + delta, eitem_);
    }
    const_iterator operator-(difference_type delta) const {
        return const_iterator(a_, i_ - delta, eitem_);
    }
    const_iterator& operator++() {
        ++i_;
        return *this;
    }
    const_iterator operator++(int) {
        ++i_;
        return const_iterator(a_, i_ - 1, eitem_);
    }
    const_iterator& operator--() {
        --i_;
        return *this;
    }
    const_iterator operator--(int) {
        --i_;
        return const_iterator(a_, i_ + 1, eitem_);
    }

    inline difference_proxy operator-(const const_iterator& x) const;

protected:
    vector_type* a_;
    size_type i_;
    TransItem* eitem_;

    bool different_end(const const_iterator& x) const {
        return eitem_ != x.eitem_;
    }
    friend class TVector<T, W>;
};

template <typename T, template <typename> typename W>
class TVector<T, W>::iterator : public const_iterator {
public:
    typedef TVector<T, W> vector_type;
    typedef typename vector_type::size_type size_type;
    typedef typename vector_type::difference_type difference_type;

    iterator() {
    }
    iterator(const TVector<T, W>* a, size_type i, TransItem* eitem)
        : const_iterator(a, i, eitem) {
    }

    typename vector_type::proxy_type operator*() const {
        return vector_type::proxy_type(this->a_, this->i_);
    }

    iterator& operator+=(difference_type delta) {
        this->i_ += delta;
        return *this;
    }
    iterator& operator-=(difference_type delta) {
        this->i_ += delta;
        return *this;
    }
    iterator operator+(difference_type delta) const {
        return iterator(this->a_, this->i_ + delta, this->eitem_);
    }
    iterator operator-(difference_type delta) const {
        return iterator(this->a_, this->i_ - delta, this->eitem_);
    }
    iterator& operator++() {
        ++this->i_;
        return *this;
    }
    iterator operator++(int) {
        ++this->i_;
        return iterator(this->a_, this->i_ - 1, this->eitem_);
    }
    iterator& operator--() {
        --this->i_;
        return *this;
    }
    iterator operator--(int) {
        --this->i_;
        return iterator(this->a_, this->i_ + 1, this->eitem_);
    }

    difference_proxy operator-(const const_iterator& x) const {
        return const_iterator::operator-(x);
    }

private:
    friend class TVector<T, W>;
};


template <typename T, template <typename> typename W>
inline auto TVector<T, W>::begin() -> iterator {
    return iterator(this, 0, 0);
}

template <typename T, template <typename> typename W>
inline auto TVector<T, W>::end() -> iterator {
    TransProxy sitem = size_item();
    return iterator(this, size_info(sitem).second, &sitem.item());
}

template <typename T, template <typename> typename W>
inline auto TVector<T, W>::cbegin() const -> const_iterator {
    return const_iterator(this, 0, 0);
}

template <typename T, template <typename> typename W>
inline auto TVector<T, W>::cend() const -> const_iterator {
    TransProxy sitem = size_item();
    return const_iterator(this, size_info(sitem).second, &sitem.item());
}

template <typename T, template <typename> typename W>
inline auto TVector<T, W>::begin() const -> const_iterator {
    return cbegin();
}

template <typename T, template <typename> typename W>
inline auto TVector<T, W>::end() const -> const_iterator {
    return cend();
}


template <typename T, template <typename> typename W>
inline auto TVector<T, W>::const_iterator::operator-(const const_iterator& x) const -> difference_proxy {
    assert(a_ == x.a_);
    if (different_end(x)) {
        TransItem* eitem = eitem_ ? eitem_ : x.eitem_;
        size_type sz = a_->original_size(eitem);
        return difference_proxy(&a_->size_predicate(eitem), sz, i_ - x.i_ - sz);
    } else
        return difference_proxy(nullptr, 0, i_ - x.i_);
}


template <typename T, template <typename> typename W>
void TVector<T, W>::clear() {
    auto sitem = size_item();
    pred_type& wval = sitem.template xwrite_value<pred_type>();
    sitem.add_write(pred_type{wval.first, 0});
}

template <typename T, template <typename> typename W>
auto TVector<T, W>::erase(iterator pos) -> iterator {
    auto sitem = size_item();
    pred_type& wval = sitem.template xwrite_value<pred_type>();
    if (pos.i_ >= wval.second)
        version_type::opaque_throw(std::out_of_range("TVector::erase"));
    size_predicate(sitem).observe(wval.first);
    for (auto idx = pos.i_; idx != wval.second - 1; ++idx)
        transPut(idx, transGet(idx + 1));
    sitem.add_write(pred_type{wval.first, wval.second - 1});
    return pos;
}

template <typename T, template <typename> typename W>
auto TVector<T, W>::insert(iterator pos, T value) -> iterator {
    auto sitem = size_item();
    pred_type& wval = sitem.template xwrite_value<pred_type>();
    if (pos.i_ > wval.second)
        version_type::opaque_throw(std::out_of_range("TVector::insert"));
    sitem.add_write(pred_type{wval.first, wval.second + 1});
    size_predicate(sitem).observe(wval.first);
    for (auto idx = wval.second - 1; idx != pos.i_; --idx)
        transPut(idx, transGet(idx - 1));
    transPut(pos.i_, std::move(value));
    return pos;
}

template <typename T, template <typename> typename W>
void TVector<T, W>::resize(size_type size, T value) {
    auto sitem = size_item();
    pred_type& wval = sitem.template xwrite_value<pred_type>();
    size_type old_size = wval.second;
    sitem.add_write(pred_type{wval.first, size});
    if (old_size < size) {
        size_predicate(sitem).observe(wval.first);
        do {
            // inlined portion of push_back (don't double-change size)
            Sto::item(this, old_size).add_write(value);
            ++old_size;
        } while (old_size < size);
    }
}

template <typename T, template <typename> typename W>
void TVector<T, W>::nontrans_reserve(size_type size) {
    size_type new_capacity = capacity_;
    while (size > new_capacity)
        new_capacity <<= 1;
    if (new_capacity > capacity_) {
        elem* new_data = reinterpret_cast<elem*>(new char[sizeof(elem) * new_capacity]);
        memcpy(new_data, data_, sizeof(elem) * capacity_);
        for (size_type i = capacity_; i != new_capacity; ++i)
            data_[i].vers = 0;
        Transaction::rcu_delete_array(reinterpret_cast<char*>(data_));
        data_ = new_data;
        capacity_ = new_capacity;
    }
}

template <typename T, template <typename> typename W>
void TVector<T, W>::print(std::ostream& w) const {
    w << "TVector<" << typeid(T).name() << ">{" << (void*) this
      << "size=" << size_.access() << '@' << size_vers_ << " [";
    for (size_type i = 0; i < size_.access(); ++i) {
        if (i)
            w << ", ";
        if (i >= 10)
            w << '[' << i << ']';
        w << data_[i].v.access() << '@' << data_[i].vers;
    }
    w << "]}";
}

template <typename T, template <typename> typename W>
std::ostream& operator<<(std::ostream& w, const TVector<T, W>& v) {
    v.print(w);
    return w;
}
