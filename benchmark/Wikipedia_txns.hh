#pragma once

#include "Wikipedia_bench.hh"

#define INTERACTIVE_TXN_START Sto::start_transaction()

#define INTERACTIVE_TXN_COMMIT \
    if (!Sto::transaction()->in_progress() || !Sto::transaction()->try_commit()) { \
        return false; \
    }

#define TXN_CHECK(op) \
    if (!(op)) { \
        Sto::transaction()->silent_abort(); \
        return false; \
    }

namespace wikipedia {

template <typename DBParams>
void wikipedia_runner<DBParams>::run_txn_addWatchList(int user_id,
                                                      int name_space,
                                                      const std::string& page_title) {
    typedef useracct_row::NamedColumn nc;

    TRANSACTION {

    bool abort, result;
    uintptr_t row;
    const void *value;

    auto wv = Sto::tx_alloc<watchlist_row>();
    bzero(wv, sizeof(watchlist_row));
    std::tie(abort, result) = db.tbl_watchlist().insert(watchlist_key(user_id, name_space, page_title), wv);
    TXN_DO(abort);

    if (name_space == 0) {
        std::tie(abort, std::ignore) = db.tbl_watchlist().insert(watchlist_key(user_id, 1, page_title), wv);
        TXN_DO(abort);
    }

    std::tie(success, result, row, value) = db.tbl_useracct().select_row(useracct_key(user_id), {{nc::user_touched, true}});
    TXN_DO(abort);
    assert(result);
    auto new_uv = Sto::tx_alloc(reinterpret_cast<const useracct_row *>(value));
    new_uv->user_touched = ig.curr_timestamp();
    db.tbl_useracct().update_row(row, new_uv);

    } RETRY(true);
}

template <typename DBParams>
void wikipedia_runner<DBParams>::run_txn_getPageAnonymous(bool for_select,
                                                          const std::string& user_ip,
                                                          int name_space,
                                                          const std::string& page_title) {
    typedef page_row::NamedColumn page_nc;
    typedef page_restrictions_row::NamedColumn pr_nc;
    typedef ipblock_row::NamedColumn ipb_nc;
    typedef revision_row::NamedColumn rev_nc;
    typedef text_row::NamedColumn text_nc;

    (void)for_select;

    TRANSACTION {

    bool abort, result;
    const void *value;

    std::tie(abort, result, std::ignore, value) = db.idx_pageid().select_row(pageid_idx_key(name_space, page_title), false);
    TXN_DO(abort);
    assert(result);
    auto page_id = reinterpret_cast<const pageid_idx_value *>(value)->page_id;

    std::tie(abort, result, std::ignore, value) = db.tbl_page().select_row(page_key(page_id), {{page_nc::page_latest, false}});
    TXN_DO(abort);
    assert(result);
    auto page_v = reinterpret_cast<const page_row *>(value);

    std::tie(abort, result, std::ignore, std::ignore) = db.tbl_page_restrictions().select_row(page_restrictions_key(page_id), {{pr_nc::pr_type, false}});
    TXN_DO(abort);
    assert(result);

    std::tie(abort, result, std::ignore, std::ignore) = db.tbl_ipblock().select_row(ipblock_key(user_ip), {{ipb_nc::ipb_expiry, false}});
    TXN_DO(abort);
    assert(result);

    auto rev_id = page_v->page_latest;
    std::tie(abort, result, std::ignore, value) = db.tbl_revision().select_row(revision_key(rev_id), {{rev_nc::rev_text_id, false}});
    TXN_DO(abort);
    assert(result);
    auto rev_text_id = reinterpret_cast<const revision_row *>(value)->rev_text_id;

    std::tie(abort, result, std::ignore, std::ignore) = db.tbl_text().select_row(text_key(rev_text_id), {{text_nc::old_text, false}, {text_nc::old_flags, false}});
    TXN_DO(abort);
    assert(result);

    } RETRY(true);
}

template <typename DBParams>
void wikipedia_runner<DBParams>::run_txn_getPageAuthenticated(bool for_select,
                                                              const std::string& user_ip,
                                                              int user_id,
                                                              int name_space,
                                                              const std::string& page_title) {
    typedef useracct_row::NamedColumn user_nc;
    typedef page_row::NamedColumn page_nc;
    typedef page_restrictions_row::NamedColumn pr_nc;
    typedef ipblock_row::NamedColumn ipb_nc;
    typedef revision_row::NamedColumn rev_nc;
    typedef text_row::NamedColumn text_nc;

    (void)for_select;

    TRANSACTION {

    bool abort, result;
    const void *value;

    std::tie(abort, result, std::ignore, std::ignore) = db.tbl_useracct().select_row(useracct_key(user_id), {{user_nc::user_name, false}});
    TXN_DO(abort);
    assert(result);

    std::tie(abort, result, std::ignore, std::ignore) = db.tbl_user_groups().select_row(user_groups_key(user_id), false);
    TXN_DO(abort);
    assert(result);

    // From this point is pretty much the same as getPageAnonymous 

    std::tie(abort, result, std::ignore, value) = db.idx_pageid().select_row(pageid_idx_key(name_space, page_title), false);
    TXN_DO(abort);
    assert(result);
    auto page_id = reinterpret_cast<const pageid_idx_value *>(value)->page_id;

    std::tie(abort, result, std::ignore, value) = db.tbl_page().select_row(page_key(page_id), {{page_nc::page_latest, false}});
    TXN_DO(abort);
    assert(result);
    auto page_v = reinterpret_cast<const page_row *>(value);

    std::tie(abort, result, std::ignore, std::ignore) = db.tbl_page_restrictions().select_row(page_restrictions_key(page_id), {{pr_nc::pr_type, false}});
    TXN_DO(abort);
    assert(result);

    std::tie(abort, result, std::ignore, std::ignore) = db.tbl_ipblock().select_row(ipblock_key(user_ip), {{ipb_nc::ipb_expiry, false}});
    TXN_DO(abort);
    assert(result);

    auto rev_id = page_v->page_latest;
    std::tie(abort, result, std::ignore, value) = db.tbl_revision().select_row(revision_key(rev_id), {{rev_nc::rev_text_id, false}});
    TXN_DO(abort);
    assert(result);
    auto rev_text_id = reinterpret_cast<const revision_row *>(value)->rev_text_id;

    std::tie(abort, result, std::ignore, std::ignore) = db.tbl_text().select_row(text_key(rev_text_id), {{text_nc::old_text, false}, {text_nc::old_flags, false}});
    TXN_DO(abort);
    assert(result);

    } RETRY(true);
}

template <typename DBParams>
void wikipedia_runner<DBParams>::run_txn_removeWatchList(int user_id,
                                                         int name_space,
                                                         const std::string& page_title) {
    typedef useracct_row::NamedColumn nc;

    TRANSACTION {

    bool abort, result;
    uintptr_t row;
    const void *value;

    std::tie(abort, result) = db.tbl_watchlist().delete_row(watchlist_key(user_id, name_space, page_title));
    TXN_DO(abort);
    assert(result);

    std::tie(abort, result, row, value) = db.tbl_useracct().select_row(useracct_key(user_id), {{nc::user_touched, true}});
    TXN_DO(abort);
    assert(result);
    auto new_uv = Sto::tx_alloc(reinterpret_cast<const useracct_row *>(value));
    new_uv->user_touched = ig.curr_timestamp();
    db.tbl_useracct().update_row(row, new_uv);

    } RETRY(true);
}

template <typename DBParams>
bool wikipedia_runner<DBParams>::txn_updatePage_inner(int text_id,
                                                      int page_id,
                                                      const std::string& page_title,
                                                      const std::string& page_text,
                                                      int page_name_space,
                                                      int user_id,
                                                      const std::string& user_ip,
                                                      const std::string& user_text,
                                                      int rev_id,
                                                      const std::string& rev_comment,
                                                      int rev_minor_edit) {
    typedef page_row::NamedColumn page_nc;
    typedef useracct_row::NamedColumn user_nc;

    INTERACTIVE_TXN_START;

    bool abort, result;
    uintptr_t row;
    const void *value;

    auto timestamp_str = ig.curr_timestamp_string();

    // INSERT NEW TEXT
    text_key new_text_k((int32_t)(db.tbl_text().gen_key()));

    auto text_v = Sto::tx_alloc<text_row>();
    text_v->old_page = page_id;
    text_v->old_flags = std::string("utf-8");
    text_v->old_text = new char[page_text.length() + 1];
    memcpy(text_v->old_text, page_text.c_str(), page_text.length() + 1);

    std::tie(abort, result) = db.tbl_text().insert_row(text_key, text_v);
    TXN_CHECK(abort);
    assert(!result);

    // INSERT NEW REVISION
    revision_key new_rev_k((int32_t)(db.tbl_revision().gen_key()));

    auto rev_v = Sto::tx_alloc<revision_row>();
    rev_v->rev_page = page_id;
    rev_v->rev_text_id = bswap(new_text_k.text_id);
    rev_v->rev_comment = rev_comment;
    rev_v->rev_minor_edit = rev_minor_edit;
    rev_v->rev_user = user_id;
    rev_v->rev_user_text = user_text;
    rev_v->rev_timestamp = timestamp_str;
    rev_v->rev_deleted = 0;
    rev_v->rev_len = page_text.length();
    rev_v->rev_parent_id = rev_id;

    std::tie(abort, result) = db.tbl_revision().insert_row(new_rev_k, rev_v);
    TXN_CHECK(abort);
    assert(!result);
    
    // UPDATE PAGE TABLE
    std::tie(abort, result, row, value) =
        db.tbl_page().select_row(page_key(page_id),
                                    {{page_nc::page_latest, true},
                                    {page_nc::page_touched, true},
                                    {page_nc::page_is_new, true},
                                    {page_nc::page_is_redirect, true},
                                    {page_nc::page_len}});
    TXN_CHECK(abort);
    assert(result);

    auto new_pv = Sto::tx_alloc(reinterpret_cast<const page_row *>(value));
    new_pv->page_latest = bswap(new_rev_k.rev_id);
    new_pv->page_touched = timestamp_str;
    new_pv->page_is_new = 0;
    new_pv->page_is_redirect = 0;
    new_pv->page_len = page_text.length();

    db.tbl_revision().update_row(row, new_pv);

    // INSERT RECENT CHANGES
    recentchanges_key rc_k((int32_t)(db.tbl_recentchanges().gen_key()));

    auto rc_v = Sto::tx_alloc<recentchanges_row>();
    bzero(rc_v, sizeof(recentchanges_row));
    rc_v->rc_timestamp = timestamp_str;
    rc_v->rc_cur_time = timestamp_str;
    rc_v->rc_namespace = page_name_space;
    rc_v->rc_title = page_title;
    rc_v->rc_type = 0;
    rc_v->rc_minor = 0;
	rc_v->rc_cur_id = page_id;
    rc_v->rc_user = user_id;
    rc_v->rc_user_text = user_text;
    rc_v->rc_comment = rev_comment;
    rc_v->rc_this_oldid = bsawp(new_text_k.text_id);
    rc_v->rc_last_oldid = text_id;
    rc_v->rc_bot = 0;
    rc_v->rc_moved_to_ns = 0;
    rc_v->rc_moved_to_title = std::string();
    rc_v->rc_ip = user_ip;
    rc_v->rc_old_len = page_text.length();
    rc_v->rc_new_len = page_text.length();

    std::tie(abort, result) = db.tbl_recentchanges().insert_row(rc_k, rc_v);
    TXN_CHECK(abort);
    assert(!result);

    // SELECT WATCHING USERS
    watchlist_idx_key k0(page_title, page_name_space, 0);
    watchlist_idx_key k1(page_title, page_name_space, std::numeric_limits<int32_t>::max());

    std::vector<int32_t> watching_users;
    auto scan_cb = [&](const watchlist_idx_key& key, const watchlist_idx_row&) {
        watching_users.push_back(key.wl_user);
    }

    abort = db.idx_watchlist().range_scan<decltype(scan_cb), false>(k0, k1, scan_cb, existence);
    TXN_CHECK(abort);

    if (!watching_users.empty()) {
        // update watchlist for each user watching
        INTERACTIVE_TXN_COMMIT;

        INTERACTIVE_TXN_START;
        for (auto& u : watching_users) {
            std::tie(abort, result, row, value) = db.tbl_watchlist().select_row(watchlist_key(u, page_name_space, page_title), true);
            TXN_CHECK(abort);
            assert(result);
            auto new_wlv = Sto::tx_alloc(reinterpret_cast<const watchlist_row *>(value));
            new_wlv->wl_notificationtimestamp = timestamp_str;
        }

        INTERACTIVE_TXN_COMMIT;

        INTERACTIVE_TXN_START;
    }

    // INSERT LOG
    logging_key lg_k(db.tbl_logging().gen_key());

    auto lg_v = Sto::tx_alloc<logging_row>();
    lg_v->log_type = std::string("patrol");
    lg_v->log_action = std::string("patrol");
    lg_v->log_timestamp = timestamp_str;
    lg_v->log_user = user_id;
    lg_v->log_namespace = page_name_space;
    lg_v->log_title = page_title;
    lg_v->log_comment = rev_comment;
    lg_v->log_params = std::string();
    lg_v->log_deleted = 0;
    lg_v->log_user_text = user_text;
    lg_v->log_page = page_id;

    std::tie(abort, result) = db.tbl_logging().insert_row(lg_k, lg_v);
    TXN_CHECK(abort);
    assert(!result);

    // UPDATE USER
    std::tie(abort, result, row, value) = db.tbl_useracct().select_row(useracct_key(user_id),
                                                                           {{user_nc::user_editcount, true},
                                                                            {user_nc::user_touched, true}});
    auto new_uv = Sto::tx_alloc(reinterpret_cast<const useracct_row *>(value));
    new_uv->user_editcount += 1;
    new_uv->user_touched = timestamp_str;
    db.tbl_useracct().update_row(row, new_uv);

    INTERACTIVE_TXN_COMMIT;

    return true;
}

template <typename DBParams>
void wikipedia_runner<DBParams>::run_txn_updatePage(int text_id,
                                                    int page_id,
                                                    const std::string& page_title,
                                                    const std::string& page_text,
                                                    int page_name_space,
                                                    int user_id,
                                                    const std::string& user_ip,
                                                    const std::string& user_text,
                                                    int rev_id,
                                                    const std::string& rev_comment,
                                                    int rev_minor_edit) {
    while (true) {
        bool success =
            txn_updatePage_inner(text_id, page_id, page_title, page_text, page_name_space, user_id,
                                 user_ip, user_text, rev_id, rev_comment, rev_minor_edit);
        if (success)
            break;
    }

}

}; // namespace wikipedia
