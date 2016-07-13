<?php

/**
 * Description of rcube_dbmail
 *
 * @author Schema31 S.P.A.
 * 
 * TO ENABLE 'rcube_dbmail' PLUGIN:
 * 1. drop rcube_dbmail.php to '../program/lib/Roundcube'
 * 2. add the following lines to roundcube/config/config.inc.php
 *    $config['storage_driver'] = 'dbmail';
 *    $config['dbmail_dsn'] = 'mysql://user:pass@host/db';  # dsn connection string
 *    $config['dbmail_hash'] = 'sha1';                      # hashing method to use, must coincide with dbmail.conf - sha1, md5, sha256, sha512, whirlpool. sha1 is the default
 *    $config['dbmail_fixed_headername_cache'] = FALSE;     # add new headernames (if not exists) in 'dbmail_headername' when saving messages
 *    $config['dbmail_cache'] = 'db';                       # Generic cache switch. FALSE (to disable cache) / 'db' / 'memcache' / 'apc'
 *    $config['dbmail_cache_ttl'] = '10d';                  # Cache default expire value
 *    $config['dbmail_sql_debug'] = FALSE;                  # log executed queries to 'logs/sql'?
 * 
 * ----------------------------
 * 
 * Notes:
 * 
 * 1. DBMAIL nightly cleanup every cached data (envelope / headers) for deleted
 *    messages, so we don't need to manually delete those records
 */
class rcube_dbmail extends rcube_storage {

    private $rc;
    private $debug = FALSE; ## Not really useful, we use it just to track internally the debug status 
    private $user_idnr = null;
    private $namespace = null;
    private $delimiter = null;
    private $dbmail = null;
    private $rcubeInstance = null;
    private $err_no = 0;
    private $err_str = '';
    private $response_code = null;

    /**
     * Searchable message headers
     */
    private $searchable_headers = array(
        'x-priority',
        'subject',
        'from',
        'date',
        'to',
        'cc',
        'bcc'
    );

    /**
     * Supported IMAP capabilities
     */
    private $imap_capabilities = array(
        'ACL' => TRUE,
        'ANNOTATE-EXPERIMENT-1' => TRUE,
        'AUTH' => TRUE,
        'BINARY' => TRUE,
        'CATENATE' => TRUE,
        'CHILDREN' => TRUE,
        'COMPRESS' => array('DEFLATE'),
        'CONDSTORE' => TRUE,
        'CONTEXT' => array('SEARCH', 'SORT'),
        'CONVERT' => TRUE,
        'CREATE-SPECIAL-USE' => TRUE,
        'ENABLE' => TRUE,
        'ESEARCH' => TRUE,
        'ESORT' => TRUE,
        'FILTERS' => TRUE,
        'I18NLEVEL' => array('1', '2'),
        'ID' => TRUE,
        'IDLE' => TRUE,
        'IMAPSIEVE' => TRUE,
        'LANGUAGE' => TRUE,
        'LIST-EXTENDED' => TRUE,
        'LIST-STATUS' => TRUE,
        'LITERAL+' => TRUE,
        'LOGIN-REFERRALS' => TRUE,
        'LOGINDISABLED' => TRUE,
        'MAILBOX-REFERRALS' => TRUE,
        'METADATA' => TRUE,
        'METADATA-SERVER' => TRUE,
        'MOVE' => TRUE,
        'MULTIAPPEND' => TRUE,
        'MULTISEARCH' => TRUE,
        'NAMESPACE' => TRUE,
        'NOTIFY' => TRUE,
        'QRESYNC' => TRUE,
        'QUOTA' => TRUE,
        'RIGHTS' => TRUE,
        'SASL-IR' => TRUE,
        'SEARCH' => array('FUZZY'),
        'SEARCHRES' => TRUE,
        'SORT' => array('DISPLAY'),
        'SPECIAL-USE' => TRUE,
        'STARTTLS' => TRUE,
        'THREAD' => array('ORDEREDSUBJECT', 'REFERENCES'),
        'UIDPLUS' => TRUE,
        'UNSELECT' => TRUE,
        'URLFETCH' => array('BINARY'),
        'URL-PARTIAL' => TRUE,
        'URLAUTH' => TRUE,
        'UTF8' => array('ACCEPT', 'ALL', 'APPEND', 'ONLY', 'USER'),
        'WITHIN' => TRUE,
    );

    /**
     * Cache configuration
     */
    protected $cache = null; // cache handler instance
    protected $caching = null; // cache type (db /memcache/...)

    /**
     *  Message status flags
     */

    const MESSAGE_STATUS_NEW = 0;
    const MESSAGE_STATUS_SEEN = 1;
    const MESSAGE_STATUS_DELETE = 2;
    const MESSAGE_STATUS_PURGE = 3;
    const MESSAGE_STATUS_UNUSED = 4;
    const MESSAGE_STATUS_INSERT = 5;
    const MESSAGE_STATUS_ERROR = 6;

    /**
     * Keyword tokens
     */
    const KEYWORD_FORWARDED = '$Forwarded';

    /**
     *  ACLs mapping flags
     */
    const ACL_CACHE_TTL = 300;
    const ACL_LOOKUP_FLAG = 'l';
    const ACL_READ_FLAG = 'r';
    const ACL_SEEN_FLAG = 's';
    const ACL_WRITE_FLAG = 'w';
    const ACL_INSERT_FLAG = 'i';
    const ACL_POST_FLAG = 'p';
    const ACL_CREATE_FLAG = 'k';
    const ACL_DELETE_FLAG = 'x';
    const ACL_DELETED_FLAG = 't';
    const ACL_EXPUNGE_FLAG = 'e';
    const ACL_ADMINISTER_FLAG = 'a';

    /**
     *  Public userId
     */
    const PUBLIC_FOLDER_USER = '__public__';

    /**
     * Temporary items time to live (seconds)
     */
    const TEMP_TTL = 300;

    public function __construct() {

        // get main roundcube instance
        $this->rcubeInstance = rcube::get_instance();
        $this->rc = rcmail::get_instance();

        // set namespaces
        if (is_null($this->namespace)) {
            $this->namespace = array(
                'personal' => array(
                    array(
                        "",
                        "/")
                ),
                'other' => array(
                    array(
                        "#Users",
                        "/"
                    )
                ),
                'shared' => array(
                    array(
                        "#Public",
                        "/"
                    )
                ),
                'prefix' => ""
            );
            $_SESSION['imap_namespace'] = $this->namespace;
        }

        // set common delimiter
        if (is_null($this->delimiter)) {
            $this->delimiter = "/";
            $_SESSION['imap_delimiter'] = $this->delimiter;
        }

        // connect to dbmail database
        if (!$this->dbmail_connect()) {
            die("Error during connection to Dbmail database: " . $this->dbmail->is_error());
        }

        // set dbmail user_idnr (retrieve it if empty)
        if (!isset($_SESSION['user_idnr']) || !$_SESSION['user_idnr'] || strlen($_SESSION['user_idnr']) == 0) {
            $_SESSION['user_idnr'] = $this->get_dbmail_user_idnr($_SESSION['username']);
        }

        $this->user_idnr = $this->get_dbmail_user_idnr($_SESSION['username']);
    }

    /**
     * This is the function that fakes the connection to the IMAP Server
     * Don't get confused by the name - it's not the function to connect to the DB Engine.
     *
     * @param  string   $host    Host to connect
     * @param  string   $user    Username for IMAP account
     * @param  string   $pass    Password for IMAP account
     * @param  integer  $port    Port to connect to
     * @param  string   $use_ssl SSL schema (either ssl or tls) or null if plain connection
     *
     * @return boolean  TRUE on success, FALSE on failure
     */
    public function connect($host, $user, $pass, $port = 143, $use_ssl = null) {

        // connected?
        if (!$this->dbmail->is_connected()) {
            return FALSE;
        }

        $valid_user = FALSE;

        // validate supplied login details
        $user_sql = "SELECT user_idnr, passwd, encryption_type "
                . " FROM dbmail_users "
                . " WHERE userid = '{$this->dbmail->escape($user)}' ";

        $res = $this->dbmail->query($user_sql);

        if ($this->dbmail->num_rows($res) == 0) {
            // usename not found
            return FALSE;
        }

        $row = $this->dbmail->fetch_assoc($res);

        // supplied password match?
        switch ($row['encryption_type']) {
            case 'md5':
                $salt = substr($row['passwd'], 0, (strrpos($row['passwd'], '$') + 1));
                $valid_user = (crypt($pass, $salt) == $row['passwd']);
                break;
            case 'md5sum':
                $valid_user = (md5($pass) == $row['passwd']);
                break;
            case 'sha1':
            case 'sha256':
            case 'sha512':
            case 'whirlpool':
                $valid_user = (hash($row['encryption_type'], $pass) == $row['passwd']);
                break;
            default :
                // plain text:
                $valid_user = ($pass == $row['passwd']);
                break;
        }

        // authenticated user?
        if (!$valid_user) {
            return FALSE;
        }

        // Update last login
        $current_datetime = new DateTime();
        $last_login_sql = "UPDATE dbmail_users "
                . "SET last_login = '{$this->dbmail->escape($current_datetime->format('Y-m-d H:i:s'))}' "
                . "WHERE user_idnr = '{$this->dbmail->escape($row['user_idnr'])}' ";

        if (!$this->dbmail->query($last_login_sql)) {
            return FALSE;
        }

        // OK - store user identity within session data
        $this->user_idnr = $row['user_idnr'];
        $_SESSION['user_idnr'] = $this->user_idnr;

        // subscribe INBOX when needed
        $mailbox_idnr = $this->get_mail_box_id('INBOX', $this->user_idnr);
        if ($mailbox_idnr && !$this->folder_subscription_exists($mailbox_idnr)) {

            // subsribe INBOX
            $this->subscribe(array('INBOX'));
        }

        return TRUE;
    }

    /**
     * Close connection. Usually done on script shutdown
     */
    public function close() {
        // DO NOTHING!!!!
    }

    /**
     * Delete all the expunged messages in all the mailboxes
     * @return boolean
     */
    public function expungeAll($mailbox_idnr) {

        // ACLs check ('expunge' grant required )
        $ACLs = $this->_get_acl(NULL, $mailbox_idnr);
        if (!is_array($ACLs) || !in_array(self::ACL_EXPUNGE_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // set message flag
        $expungeAllSQL = "UPDATE dbmail_mailboxes "
                . "INNER JOIN dbmail_messages ON dbmail_messages.mailbox_idnr = dbmail_mailboxes.mailbox_idnr "
                . "SET dbmail_messages.status = 2, "
                . "dbmail_mailboxes.seq = dbmail_mailboxes.seq + 1 "
                . "WHERE dbmail_messages.deleted_flag = 1 "
                . "AND dbmail_mailboxes.mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} "
                . "AND dbmail_messages.status < {$this->dbmail->escape(self::MESSAGE_STATUS_DELETE)} "
                . "AND dbmail_mailboxes.owner_idnr = {$this->dbmail->escape($this->user_idnr)} ";

        return ($this->dbmail->query($expungeAllSQL) ? TRUE : FALSE);
    }

    /**
     * Checks connection state.
     *
     * @return boolean  TRUE on success, FALSE on failure
     */
    public function is_connected() {
        return $this->dbmail->is_connected();
    }

    /**
     * Check connection state, connect if not connected.
     *
     * @return bool Connection state.
     */
    public function check_connection() {
        return $this->dbmail->is_connected();
    }

    /**
     * Returns code of last error
     *
     * @return int Error code
     */
    public function get_error_code() {
        return $this->err_no;
    }

    /**
     * Returns message of last error
     *
     * @return string Error message
     */
    public function get_error_str() {
        return $this->err_str;
    }

    /**
     * Returns code of last command response
     *
     * @return int Response code (class constant)
     */
    public function get_response_code() {
        return $this->response_code;
    }

    /**
     * Set connection and class options
     *
     * @param array $opt Options array
     */
    public function set_options($opt) {
        $this->options = array_merge($this->options, (array) $opt);
    }

    /**
     * Get connection/class option
     *
     * @param string $name Option name
     *
     * @param mixed Option value
     */
    public function get_option($name) {
        return $this->options[$name];
    }

    /**
     * Activate/deactivate debug mode.
     *
     * @param boolean $dbg True if conversation with the server should be logged
     */
    public function set_debug($dbg = true) {

        /** Enable Query Logging * */
        $this->debug($dbg);
        $this->dbmail->set_debug($dbg);
    }

    /**
     * Set default message charset.
     *
     * This will be used for message decoding if a charset specification is not available
     *
     * @param  string $cs Charset string
     */
    public function set_charset($cs) {
        $this->default_charset = $cs;
    }

    /**
     * Set internal folder reference.
     * All operations will be perfomed on this folder.
     *
     * @param  string $folder  Folder name
     */
    public function set_folder($folder) {
        if ($this->folder === $folder) {
            return;
        }

        $this->folder = $folder;
    }

    /**
     * Backward compatibility with IMAP stuff
     *
     *  set_mailbox == set_folder
     */
    public function set_mailbox($folder) {
        $this->set_folder($folder);
    }

    /**
     * Returns the currently used folder name
     *
     * @return string Name of the folder
     */
    public function get_folder() {
        return $this->folder;
    }

    /**
     * Set internal list page number.
     *
     * @param int $page Page number to list
     */
    public function set_page($page) {
        $this->list_page = (int) $page;
    }

    /**
     * Gets internal list page number.
     *
     * @return int Page number
     */
    public function get_page() {
        return $this->list_page;
    }

    /**
     * Set internal page size
     *
     * @param int $size Number of messages to display on one page
     */
    public function set_pagesize($size) {
        $this->page_size = (int) $size;
    }

    /**
     * Get internal page size
     *
     * @return int Number of messages to display on one page
     */
    public function get_pagesize() {
        return $this->page_size;
    }

    /**
     * Save a search result for future message listing methods.
     *
     * @param  mixed  $set  Search set in driver specific format
     */
    public function set_search_set($set) {

        // $this->search_set = $set;

        $set = (array) $set;

        $this->search_string = $set[0];
        $this->search_set = $set[1];
        $this->search_charset = $set[2];
        $this->search_sort_field = $set[3];
        $this->search_sorted = $set[4];
        $this->search_threads = is_a($this->search_set, 'rcube_result_thread');

        if (is_a($this->search_set, 'rcube_result_multifolder')) {
            $this->set_threading(false);
        }
    }

    /**
     * Return the saved search set.
     *
     * @return array Search set in driver specific format, NULL if search wasn't initialized
     */
    public function get_search_set() {

        if (empty($this->search_set)) {
            return null;
        }

        return array(
            $this->search_string,
            $this->search_set,
            $this->search_charset,
            $this->search_sort_field,
            $this->search_sorted,
        );
    }

    /**
     * Returns the storage server's (IMAP) capability
     *
     * @param   string  $cap Capability name
     *
     * @return  mixed   Capability value or TRUE if supported, FALSE if not
     */
    public function get_capability($cap) {

        $cap = strtoupper($cap);

        /*
         * Supported capability?
         */
        if (!in_array($cap, $this->imap_capabilities)) {
            /*
             * Not found!
             */
            return FALSE;
        } elseif (is_array($this->imap_capabilities[$cap]) && count($this->imap_capabilities[$cap]) > 0) {
            /*
             * Key / value pairs found: return supported capability properties
             */
            return $this->imap_capabilities[$cap];
        } else {
            /*
             * Supported
             */
            return TRUE;
        }
    }

    /**
     * Sets threading flag to the best supported THREAD algorithm.
     * Enable/Disable threaded mode.
     *
     * @param  boolean  $enable TRUE to enable and FALSE
     *
     * @return mixed   Threading algorithm or False if THREAD is not supported
     */
    public function set_threading($enable = false) {

        $this->threading = false;

        if (!$enable) {
            return $this->threading;
        }

        $caps = $this->get_capability('THREAD');
        if (!$caps || !is_array($caps)) {
            return $this->threading;
        }

        $methods = array_intersect(array('REFS', 'REFERENCES', 'ORDEREDSUBJECT'), $caps);
        if (!is_array($methods) || count($methods) == 0) {
            return $this->threading;
        }

        $this->threading = array_shift($methods);

        return $this->threading;
    }

    /**
     * Get current threading flag.
     *
     * @return mixed  Threading algorithm or False if THREAD is not supported or disabled
     */
    public function get_threading() {

        return $this->threading;
    }

    /**
     * Checks the PERMANENTFLAGS capability of the current folder
     * and returns true if the given flag is supported by the server.
     *
     * @param   string  $flag Permanentflag name
     *
     * @return  boolean True if this flag is supported
     */
    public function check_permflag($flag) {
        // TO DO!!!!!!
    }

    /**
     * Returns the delimiter that is used by the server
     * for folder hierarchy separation.
     *
     * @return  string  Delimiter string
     */
    public function get_hierarchy_delimiter() {
        return $this->delimiter;
    }

    /**
     * Get namespace
     *
     * @param string $name Namespace array index: personal, other, shared, prefix
     *
     * @return  array  Namespace data
     */
    public function get_namespace($name = null) {

        $ns = $this->namespace;

        if ($name) {
            return (array_key_exists($name, $ns) ? $ns[$name] : null);
        }

        unset($ns['prefix']);
        return $ns;
    }

    /**
     * Get message count for a specific folder
     *
     * @param  string  $folder  Folder name
     * @param  string  $mode    Mode for count [ALL|THREADS|UNSEEN|RECENT|EXISTS]
     * @param  boolean $force   Force reading from server and update cache
     * @param  boolean $status  Enables storing folder status info (max UID/count),
     *                          required for folder_status()
     * @return int Number of messages
     */
    public function count($folder = null, $mode = 'ALL', $force = false, $status = true) {

        /*
         * Normalize target mailbox/s
         */
        if (is_array($folder) && count($folder) > 0) {
            // mailboxes list supplied
            $target = $folder;
        } elseif (is_string($folder) && strlen($folder) > 0) {
            // single mailbox supplied
            $target = array($folder);
        } elseif (array_key_exists('search_scope', $_SESSION) && $_SESSION['search_scope'] == 'all') {
            // no mailbox supplied, search within all mailboxes
            $target = $this->list_folders_subscribed('', '*', 'mail', null, true);
        } else if (array_key_exists('search_scope', $_SESSION) && $_SESSION['search_scope'] == 'sub') {
            // no mailbox supplied, search within current mailbox and nested ones
            $target = $this->list_folders_subscribed($this->folder, '*', 'mail');
        } elseif (strlen($this->folder) > 0) {
            $target = array($this->folder);
        }

        /*
         * Do we have something?
         */
        if (!is_array($target) || count($target) == 0) {
            // empty set!!!
            return 0;
        }

        $folders = $this->_format_folders_list($target);

        /*
         * Map mailboxes ID
         */
        $mailboxes = array();
        $seqFlags = array();
        foreach ($folders as $folder_name) {

            /*
             * Retrieve mailbox ID
             */
            $mail_box_idnr = $this->get_mail_box_id($folder_name);
            if (!$mail_box_idnr) {
                // Not found - Skip!
                return FALSE;
            }

            /*
             * ACLs check ('lookup' and 'read' grants required )
             */
            $ACLs = $this->_get_acl(NULL, $mail_box_idnr);
            if (!is_array($ACLs) || !in_array(self::ACL_LOOKUP_FLAG, $ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
                // Unauthorized - Skip!
                return FALSE;
            }

            /*
             * Add mailbox ID to mailboxes list
             */
            $mailboxes[$mail_box_idnr] = $folder_name;

            /*
             * Retrieve mailbox current 'seq' flag (we will use this list to build 
             * cache key to prevent using an outdated value!)
             */
            $seqFlags[$mail_box_idnr] = $this->get_mailbox_seq(key($mailboxes));
        }

        /*
         * Valid mailboxes supplied?
         */
        if (!is_array($mailboxes) || count($mailboxes) == 0) {
            // empty set!!!
            return 0;
        }

        /*
         * Set cache key
         */
        $token = $mode;
        $token .= serialize($seqFlags);
        $token .= isset($this->search_set[0]) ? $this->search_set[0] : '';
        $rcmh_cached_key = "MAILBOX_COUNT_" . md5($token);

        $rcmh_cached = $this->get_cache($rcmh_cached_key);
        if ($rcmh_cached) {
            /*
             * Return cached content
             */
            return $rcmh_cached;
        }

        /*
         * Init $additional_joins list
         */
        $additional_joins = '';

        /*
         * Retrieve search string 
         */
        $search_str = NULL;
        if (is_array($this->search_set) && array_key_exists(0, $this->search_set)) {
            $tmp = $this->format_search_parameters($this->search_set[0]);
            $search_str = $tmp->search;
        }

        $search_conditions = $this->_translate_search_parameters($search_str);
        if (!$search_conditions) {
            return FALSE;
        }

        /*
         *  set additional join tables according to supplied search / filter conditions
         */
        if (is_object($search_conditions) && property_exists($search_conditions, 'additional_join_tables')) {
            $additional_joins .= " {$search_conditions->additional_join_tables} ";
        }

        /*
         * Set base 'where' conditions
         */
        $where_conditions = " WHERE dbmail_messages.mailbox_idnr IN (" . implode(",", array_keys($mailboxes)) . ")";
        $where_conditions .= " AND dbmail_messages.status < " . self::MESSAGE_STATUS_DELETE . " ";

        /*
         * Apply search criteria
         */
        if (isset($search_conditions->additional_where_conditions) && strlen($search_conditions->additional_where_conditions) > 0) {
            $where_conditions .= " AND {$search_conditions->additional_where_conditions}";
            $additional_joins .= implode(PHP_EOL, $search_conditions->additional_joins);
        }

        /*
         *  add 'where' conditions according to supplied search / filter conditions
         */
        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_filter_str') && strlen($search_conditions->formatted_filter_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_filter_str} ) ";
        }

        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_search_str') && strlen($search_conditions->formatted_search_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_search_str} ) ";
        }

        if ($mode == 'UNSEEN') {
            $where_conditions .= " AND dbmail_messages.seen_flag = 0 ";
        }

        /*
         * Prepare base query.
         */
        if ($mode == 'THREADS') {
            /*
             * When counting threads, we should retrieve additional details, so we use a different query
             */
            $query = " SELECT mailbox_idnr, dbmail_messages.message_idnr, dbmail_messages.physmessage_id ";
        } elseif (isset($search_conditions->needs_physmessages) && $search_conditions->needs_physmessages) {
            /*
             * When joining additional tables, use a distinct clause to avoid duplicated items
             */
            $query = " SELECT count(DISTINCT dbmail_messages.message_idnr) as tot ";
        } else {
            $query = " SELECT count(dbmail_messages.message_idnr) as tot ";
        }

        $query .= " FROM dbmail_messages ";

        /*
         * Join to dbmail_physmessage when needed 
         */
        if (isset($search_conditions->needs_physmessages) && $search_conditions->needs_physmessages) {
            $query .= " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id ";
        }

        $query .= " {$additional_joins} ";
        $query .= " {$where_conditions} ";


        /*
         * Execute query
         */
        $res = $this->dbmail->query($query);

        /*
         * Init counter
         */
        $count = 0;

        if ($mode == 'THREADS') {

            $threads = array();
            while ($row = $this->dbmail->fetch_assoc($res)) {

                /*
                 * Retrieve thread details (base message identifier and ancesters list)
                 */
                $thread_details = $this->_get_thread_details($row['physmessage_id'], $row['message_idnr'], $mailboxes[$row['mailbox_idnr']]);
                if (!in_array($thread_details->base_thread_message_id, $threads)) {
                    $count++;
                    $threads[] = $thread_details->base_thread_message_id;
                }
            }
        } else {
            $row = $this->dbmail->fetch_assoc($res);

            $count = isset($row['tot']) && (int) $row['tot'] > 0 ? (int) $row['tot'] : 0;
        }

        /*
         * Write to cache
         */
        $this->update_cache($rcmh_cached_key, $count);

        /*
         * Set 'folder_status' when needed
         */
        if ($mode == 'ALL' && $status) {
            $target_folder_name = array_pop($mailboxes);
            $this->set_folder_stats($target_folder_name, 'cnt', $count);
            $this->set_folder_stats($target_folder_name, 'maxuid', ($count > 0 ? $this->get_latest_message_idnr($target_folder_name) : 0));
        }

        return $count;
    }

    /**
     * Get latest message ID within specific folder.
     *
     * @param  string  $folder  Folder name    
     * @return int     message_idnr
     */
    public function get_latest_message_idnr($folder = null) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        // mailbox exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        // ACLs check ('lookup' and 'read' grants required )
        $ACLs = $this->_get_acl(NULL, $mailbox_idnr);
        if (!is_array($ACLs) || !in_array(self::ACL_LOOKUP_FLAG, $ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // prepare base query
        $query = " SELECT MAX(message_idnr) AS latest_message_idnr "
                . " FROM dbmail_messages "
                . " WHERE dbmail_messages.mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} "
                . " AND dbmail_messages.status < " . self::MESSAGE_STATUS_DELETE;


        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        return ($row['latest_message_idnr'] > 0 ? $row['latest_message_idnr'] : FALSE);
    }

    /**
     * Public method for listing message flags
     *
     * @param string $folder  Folder name
     * @param array  $uids    Message UIDs
     * @param int    $mod_seq Optional MODSEQ value
     *
     * @return array Indexed array with message flags
     */
    public function list_flags($folder, $uids, $mod_seq = null) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        $mailbox_idnr = $this->get_mail_box_id($folder);

        /*
         * Filter by:
         * 1 - $mailbox_idnr
         * 2 - $message_idnrs
         * 3 - $mailbox_idnr + $message_idnrs
         */
        $filters = array();

        if (strlen($mailbox_idnr) > 0) {
            $filters[] = "mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)}";
        }

        if (is_array($uids) && count($uids) > 0) {

            foreach ($uids as &$uid) {
                /*
                 *  escape arguments
                 */
                $uid = $this->dbmail->escape($uid);
            }
            $filters[] = "message_idnr in (" . implode(',', $uids) . ")";
        }

        if (count($filters) == 0) {
            /*
             * No filters supplied!
             */
            return array();
        }

        $query = " SELECT message_idnr, seen_flag, answered_flag, deleted_flag, flagged_flag, recent_flag, draft_flag "
                . " FROM dbmail_messages "
                . " WHERE " . implode(" AND ", $filters);

        $res = $this->dbmail->query($query);

        $result = array();
        while ($row = $this->dbmail->fetch_assoc($res)) {

            $result[$row['message_idnr']] = array(
                'seen' => ($row['seen_flag'] ? TRUE : FALSE),
                'answered' => ($row['answered_flag'] ? TRUE : FALSE),
                'deleted' => ($row['deleted_flag'] ? TRUE : FALSE),
                'flagged' => ($row['flagged_flag'] ? TRUE : FALSE),
                'recent' => ($row['recent_flag'] ? TRUE : FALSE),
                'draft' => ($row['draft_flag'] ? TRUE : FALSE)
            );
        }

        return $result;
    }

    /**
     * Public method for listing headers.
     *
     * @param   mixed    $folder     Folders list
     * @param   int      $page       Current page to list
     * @param   string   $sort_field Header field to sort by
     * @param   string   $sort_order Sort order [ASC|DESC]
     * @param   int      $slice      Number of slice items to extract from result array
     *
     * @return  array    Indexed array with message header objects
     */
    public function list_messages($folder = null, $page = null, $sort_field = null, $sort_order = null, $slice = 0) {

        $target = array();

        if (is_array($folder) && count($folder) > 0) {
            // mailboxes list supplied
            $target = $folder;
        } elseif (is_string($folder) && strlen($folder) > 0) {
            // single mailbox supplied
            $target = array($folder);
        } elseif (array_key_exists('search_scope', $_SESSION) && $_SESSION['search_scope'] == 'all') {
            // no mailbox supplied, search within all mailboxes
            $target = $this->list_folders_subscribed('', '*', 'mail', null, true);
        } else if (array_key_exists('search_scope', $_SESSION) && $_SESSION['search_scope'] == 'sub') {
            // no mailbox supplied, search within current mailbox and nested ones
            $target = $this->list_folders_subscribed($this->folder, '*', 'mail');
        }

        if (!is_array($target) || count($target) == 0) {
            // empty set!!!
            return array();
        }

        $folders = $this->_format_folders_list($target);

        $search_str = isset($this->search_set[0]) ? $this->search_set[0] : '';

        return $this->_list_messages($folders, $page, $sort_field, $sort_order, $slice, $search_str, NULL);
    }

    /**
     * Return sorted list of message UIDs
     *
     * @param mixed     $folder     Folders list to get index from
     * @param string    $sort_field Sort column
     * @param string    $sort_order Sort order [ASC, DESC]
     *
     * @return rcube_result_index|rcube_result_thread List of messages (UIDs)
     */
    public function index($folder = null, $sort_field = null, $sort_order = null) {

        $folders = $this->_format_folders_list($folder);

        // get messages list
        $result_index_str = "";
        $messages = $this->_list_messages($folders, 0, $sort_field, $sort_order);
        foreach ($messages as $message) {
            $result_index_str .= " {$message->uid}";
        }

        $index = new rcube_result_index($folder, "* SORT {$result_index_str}");

        return $index;
    }

    /**
     * Invoke search request to the server.
     *
     * @param  mixed   $folder     Folders list to search in
     * @param  string  $str        Search criteria
     * @param  string  $charset    Search charset
     * @param  string  $sort_field Header field to sort by
     *
     * @todo: Search criteria should be provided in non-IMAP format, eg. array
     */
    public function search($folder = null, $str = 'ALL', $charset = null, $sort_field = null) {

        /*
         * Here we only init rcube_result_index instance, no need to execute search (done within list_messages() method)
         */

        $folders = $this->_format_folders_list($folder);

        $index = new rcube_result_index($folders, NULL);

        $this->search_set = array(
            $str,
            $index
        );

        return $index;
    }

    /**
     * Direct (real and simple) search request (without result sorting and caching).
     *
     * @param  mixed    $folder     Folders list to search in
     * @param  string   $str        Search string
     *
     * @return rcube_result_index  Search result (UIDs)
     */
    public function search_once($folder = null, $str = 'ALL') {

        /*
         * Here we only init rcube_result_index instance, no need to execute search (done within list_messages() method)
         */

        $folders = $this->_format_folders_list($folder);

        $index = new rcube_result_index($folders, NULL);

        $this->search_set = array(
            $str,
            $index
        );

        return $index;
    }

    /**
     * Refresh saved search set
     *
     * @return array Current search set
     */
    public function refresh_search() {

        if (!empty($this->search_string)) {

            $folder = (is_object($this->search_set) ? $this->search_set->get_parameters('MAILBOX') : '');

            $this->search($folder, $this->search_string, $this->search_charset, $this->search_sort_field);
        }

        return $this->get_search_set();
    }

    /* --------------------------------
     *        messages management
     * -------------------------------- */

    /**
     * Fetch message headers and body structure from the server and build
     * an object structure similar to the one generated by PEAR::Mail_mimeDecode
     *
     * @param int     $uid     Message UID to fetch
     * @param string  $folder  Folder to read from
     *
     * @return object rcube_message_header Message data
     */
    public function get_message($uid, $folder = null) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        // Retrieve message record
        $message_metadata = $this->get_message_record($uid);
        if (!$message_metadata) {
            // not found
            return FALSE;
        }

        // ACLs check ('read' grant required )
        $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
        if (!is_array($ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        return $this->retrieve_message($uid, $folder);
    }

    /**
     * Return message headers object of a specific message
     *
     * @param int     $id       Message sequence ID or UID
     * @param string  $folder   Folder to read from
     * @param bool    $force    True to skip cache
     *
     * @return rcube_message_header Message headers
     */
    public function get_message_headers($uid, $folder = null, $force = false) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        // Retrieve message record
        $message_metadata = $this->get_message_record($uid);
        if (!$message_metadata) {
            // not found
            return FALSE;
        }

        // ACLs check ('read' grant required )
        $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
        if (!is_array($ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        return $this->retrieve_message($uid, $folder);
    }

    /**
     * Fetch message body of a specific message from the server
     *
     * @param  int                $uid    Message UID
     * @param  string             $part   Part number
     * @param  rcube_message_part $o_part Part object created by get_structure()
     * @param  mixed              $print  True to print part, ressource to write part contents in
     * @param  resource           $fp     File pointer to save the message part
     * @param  boolean            $skip_charset_conv Disables charset conversion
     *
     * @return string Message/part body if not printed
     */
    public function get_message_part($uid, $part = 1, $o_part = null, $print = null, $fp = null, $skip_charset_conv = false) {

        // Retrieve message record
        $message_metadata = $this->get_message_record($uid);
        if (!$message_metadata) {
            // not found
            return FALSE;
        }

        // ACLs check ('read' grant required )
        $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
        if (!is_array($ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // Get mime content
        $mime = $this->fetch_part_lists($message_metadata['physmessage_id']);

        // Decode raw message
        $mime_decoded = $this->decode_raw_message($mime->header . $mime->body);
        if (!$mime_decoded) {
            return FALSE;
        }

        // Get message body
        $body = $this->get_message_part_body($mime_decoded, $part);

        if ($print) {
            // Print message!
            echo $body;
        }
        return $body;
    }

    /**
     * Fetch message body of a specific message from the server
     *
     * @param  int    $uid  Message UID
     *
     * @return string $part Message/part body
     * @see    rcube_imap::get_message_part()
     */
    public function get_body($uid, $part = 1) {

        $rcube_message_header = $this->get_message_headers($uid);
        if (!$rcube_message_header) {
            // not found
            return FALSE;
        }

        // ACLs checks within method get_message_part()!!!!

        return rcube_charset::convert($this->get_message_part($uid, $part, null), $rcube_message_header->charset ? $rcube_message_header->charset : $rcube_message_header->default_charset);
    }

    /**
     * Returns the whole message source as string (or saves to a file)
     *
     * @param int      $uid  Message UID
     * @param resource $fp   File pointer to save the message
     * @param string   $part Optional message part ID
     *
     * @return string Message source string
     */
    public function get_raw_body($uid, $fp = null, $part = null) {

        // Retrieve message record
        $message_metadata = $this->get_message_record($uid);
        if (!$message_metadata) {
            // not found
            return FALSE;
        }

        // ACLs check ('read' grant required )
        $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
        if (!is_array($ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // extract mime parts
        $mime = $this->fetch_part_lists($message_metadata['physmessage_id'], $part);

        return $mime->body;
    }

    /**
     * Returns the message headers as string
     *
     * @param int    $uid  Message UID
     * @param string $part Optional message part ID
     *
     * @return string Message headers string
     */
    public function get_raw_headers($uid, $part = null) {

        // Retrieve message record
        $message_metadata = $this->get_message_record($uid);
        if (!$message_metadata) {
            // not found
            return FALSE;
        }

        // ACLs check ('read' grant required )
        $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
        if (!is_array($ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // extract mime parts
        $mime = $this->fetch_part_lists($message_metadata['physmessage_id'], $part);

        return $mime->header;
    }

    /**
     * Sends the whole message source to stdout
     *
     * @param int  $uid       Message UID
     * @param bool $formatted Enables line-ending formatting
     */
    public function print_raw_body($uid, $formatted = true) {

        // ACLs checks within get_raw_headers() and get_raw_body() methods!!!

        echo ($this->get_raw_headers($uid));
        echo ($this->get_raw_body($uid));

        return TRUE;
    }

    /**
     * Set message flag to one or several messages
     *
     * @param mixed   $uids       Message UIDs as array or comma-separated string, or '*'
     * @param string  $flag       Flag to set: UNDELETED, DELETED, SEEN, UNSEEN, FLAGGED, UNFLAGGED, ANSWERED, FORWARDED
     * @param string  $folder     Folder name
     * @param boolean $skip_cache True to skip message cache clean up
     *
     * @return bool  Operation status
     */
    public function set_flag($uids, $flag, $folder = null, $skip_cache = false) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }
        // format supplied message UIDs list
        $message_uids = $this->list_message_UIDs($uids, $folder);
        if (!$message_uids) {
            return FALSE;
        }

        // some flags (es. FORWARDED) must be stored within 'dbmail_keywords' table insted of 'dbmail_messages'
        $is_keyword = FALSE;
        $keyword_token = NULL;

        // validate target flag
        $flag_field = '';
        $flag_value = '';
        $msg_flag_key = '';
        $msg_flag_value = '';
        $required_ACL = '';
        switch ($flag) {
            case 'UNDELETED':
                $is_keyword = FALSE;
                $keyword_token = NULL;
                $flag_field = 'deleted_flag';
                $flag_value = 0;
                $msg_flag_key = 'DELETED';
                $msg_flag_value = 0;
                $required_ACL = self::ACL_DELETED_FLAG;
                break;
            case 'DELETED':
                $is_keyword = FALSE;
                $keyword_token = NULL;
                $flag_field = 'deleted_flag';
                $flag_value = 1;
                $msg_flag_key = 'DELETED';
                $msg_flag_value = 1;
                $required_ACL = self::ACL_DELETED_FLAG;
                break;
            case 'UNSEEN':
                $is_keyword = FALSE;
                $keyword_token = NULL;
                $flag_field = 'seen_flag';
                $flag_value = 0;
                $msg_flag_key = 'SEEN';
                $msg_flag_value = 0;
                $required_ACL = self::ACL_SEEN_FLAG;
                break;
            case 'SEEN':
                $is_keyword = FALSE;
                $keyword_token = NULL;
                $flag_field = 'seen_flag';
                $flag_value = 1;
                $msg_flag_key = 'SEEN';
                $msg_flag_value = 1;
                $required_ACL = self::ACL_SEEN_FLAG;
                break;
            case 'UNFLAGGED':
                $is_keyword = FALSE;
                $keyword_token = NULL;
                $flag_field = 'flagged_flag';
                $flag_value = 0;
                $msg_flag_key = 'FLAGGED';
                $msg_flag_value = 0;
                $required_ACL = self::ACL_WRITE_FLAG;
                break;
            case 'FLAGGED':
                $is_keyword = FALSE;
                $keyword_token = NULL;
                $flag_field = 'flagged_flag';
                $flag_value = 1;
                $msg_flag_key = 'FLAGGED';
                $msg_flag_value = 1;
                $required_ACL = self::ACL_WRITE_FLAG;
                break;
            case 'ANSWERED':
                $is_keyword = FALSE;
                $keyword_token = NULL;
                $flag_field = 'answered_flag';
                $flag_value = 1;
                $msg_flag_key = 'ANSWERED';
                $msg_flag_value = 1;
                $required_ACL = self::ACL_WRITE_FLAG;
                break;
            case 'FORWARDED':
                $is_keyword = TRUE;
                $keyword_token = self::KEYWORD_FORWARDED;
                $flag_field = NULL;
                $flag_value = NULL;
                $msg_flag_key = 'FORWARDED';
                $msg_flag_value = 1;
                $required_ACL = self::ACL_WRITE_FLAG;
                break;
            default:
                // invalid flag supplied
                return FALSE;
        }

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        // loop messages
        foreach ($message_uids as $message_uid) {

            // Retrieve message record
            $message_metadata = $this->get_message_record($message_uid);
            if (!$message_metadata) {
                // not found
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // ACLs checks
            $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
            if (!is_array($ACLs) || !in_array($required_ACL, $ACLs)) {
                // Unauthorized!
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // set message flag
            if (!$is_keyword) {

                // update 'dbmail_messages' table
                $query = "UPDATE dbmail_messages "
                        . "SET {$this->dbmail->escape($flag_field)} = {$this->dbmail->escape($flag_value)} "
                        . "WHERE message_idnr = {$this->dbmail->escape($message_uid)} ";

                if (!$this->dbmail->query($query)) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }
            } else {

                // update 'dbmail_keywords' table
                if (!$this->set_keyword($message_uid, $keyword_token)) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }
            }

            // update cached message flag (if needed)
            $rcmh_cached_key = "MSG_" . $message_uid;
            $rcmh_cached = $this->get_cache($rcmh_cached_key);

            if (!$skip_cache &&
                    is_object($rcmh_cached) &&
                    property_exists($rcmh_cached, 'flags') &&
                    is_array($rcmh_cached->flags)) {
                // cached message found - update flags!
                $rcmh_cached->flags[$msg_flag_key] = $msg_flag_value;

                // update cached contents
                $this->update_cache($rcmh_cached_key, $rcmh_cached);
            }
        }

        //increment seq
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            // not found
            return FALSE;
        }
        $this->increment_mailbox_seq($this->dbmail->escape($mailbox_idnr));

        // return status
        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    /**
     * Remove message flag for one or several messages
     *
     * @param mixed  $uids    Message UIDs as array or comma-separated string, or '*'
     * @param string $flag    Flag to unset: SEEN, DELETED, RECENT, ANSWERED, DRAFT, MDNSENT
     * @param string $folder  Folder name
     *
     * @return bool   Operation status
     * @see set_flag
     */
    public function unset_flag($uids, $flag, $folder = null) {

        return $this->set_flag($uids, 'UN' . $flag, $folder);
    }

    /**
     * Append a mail message (source) to a specific folder.
     *
     * @param string       $folder  Target folder
     * @param string|array $message The message source string or filename
     *                              or array (of strings and file pointers)
     * @param string       $headers Headers string if $message contains only the body
     * @param boolean      $is_file True if $message is a filename
     * @param array        $flags   Message flags
     * @param mixed        $date    Message internal date
     * @ref   dm_message.c - function dbmail_message_store()
     *
     * @return int|bool Appended message UID or True on success, False on error
     */
    public function save_message($folder, &$message, $headers = '', $is_file = false, $flags = array(), $date = null) {

        /*

          Function general outline

          1 Find folder                          - function db_find_create_mailbox
          2 Create physical message  (Dedup??)   - function insert_physmessage
          3 Create (logical) message (step == 0) - function _message_insert
          4 Update USER Quota        (step == 1) - function _update_message
          5 Save message parts       (step == 2) - function dm_message_store
          6 Update headers cache     (step == 3) - function dbmail_message_cache_headers
          7 Update envelope cache                - function dbmail_message_cache_envelope
          8 Update reference field

         */

        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        /*
         *  1 Find folder
         */
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            // not found
            return FALSE;
        }

        // ACLs check ('insert' grant required )
        $ACLs = $this->_get_acl($folder);
        if (!is_array($ACLs) || !in_array(self::ACL_INSERT_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        /*
         *  2 Create physical message
         */
        $query = "INSERT INTO dbmail_physmessage "
                . " ( "
                . "    messagesize, "
                . "    rfcsize, "
                . "    internal_date "
                . " ) "
                . " VALUES "
                . " ( "
                . "    '{$this->dbmail->escape(strlen($message))}', "
                . "    '{$this->dbmail->escape(strlen($message))}', "
                . "    NOW() "
                . " ) ";

        if (!$this->dbmail->query($query)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // Retrieve inserted ID
        $physmessage_id = $this->dbmail->insert_id('dbmail_physmessage');
        if (!$physmessage_id) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        /*
         *  3 Create (logical) message
         */
        $query = "INSERT INTO dbmail_messages "
                . " ( "
                . "    mailbox_idnr, "
                . "    physmessage_id, "
                . "    seen_flag, "
                . "    unique_id, "
                . "    status "
                . " ) "
                . " VALUES "
                . " ("
                . "    '{$this->dbmail->escape($mailbox_idnr)}', "
                . "    '{$this->dbmail->escape($physmessage_id)}', "
                . "    1, "
                . "    '{$this->dbmail->escape($this->create_message_unique_id())}', "
                . "    '{$this->dbmail->escape(self::MESSAGE_STATUS_SEEN)}' "
                . " )";

        if (!$this->dbmail->query($query)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // Retrieve inserted ID
        $message_idnr = $this->dbmail->insert_id('dbmail_messages');
        if (!$message_idnr) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        /*
         *  4 Update USER Quota
         */
        if (!$this->increment_user_quota($this->user_idnr, strlen($message))) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // create Mail_mimeDecode obj
        $mime_decoded = $this->decode_raw_message($message, FALSE);
        if (!$mime_decoded) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        /*
         * 5 Save message parts
         */

        // store main mail headers (retrieve real message headers instead of using Mail_mimeDecode content which are lowercased!!!!)
        $real_headers = $this->extract_raw_headers_from_message($message);
        if (!$this->_part_insert($physmessage_id, $real_headers, 1, 1, 0, 0)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // store mime parts
        $part_key = 1;  ## this must be in a variable because it's passed by reference
        if (!$this->store_mime_object($physmessage_id, $mime_decoded, $part_key, 0, 1)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        /*
         * 6 Update headers cache
         */

        // set raw headers string and store searchable header name /value pairs
        $raw_headers = '';
        foreach ($mime_decoded->headers as $header_name => $header_value) {
            $raw_headers .= $header_name . $this->get_header_delimiter($header_name) . $header_value . "\n";
        }

        // filter searchable headers and save them
        $searchable_headers = $this->get_searchable_headers($raw_headers);
        foreach ($searchable_headers as $header_name => $header_value) {

            if (!$this->save_searchable_header($physmessage_id, $header_name, $header_value)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        /*
         * 7 Update envelope cache 
         */
        $envelope_headers = $this->get_mail_envelope_headers($raw_headers);
        if (!$this->save_mail_envelope_headers($physmessage_id, $envelope_headers)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // increment folder and message 'seq' flag
        if (!$this->increment_mailbox_seq($mailbox_idnr, $message_idnr)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        return ($this->dbmail->endTransaction() ? $message_idnr : FALSE);
    }

    /**
     * Move message(s) from one folder to another.
     *
     * @param mixed  $uids  Message UIDs as array or comma-separated string, or '*'
     * @param string $to    Target folder
     * @param string $from  Source folder
     *
     * @return boolean True on success, False on error
     */
    public function move_message($uids, $to, $from = null) {

        // destination folder exists?
        $to_mailbox_idnr = $this->get_mail_box_id($to);
        if (!$to_mailbox_idnr) {
            return FALSE;
        }

        // ACLs check ('insert' grant required for destination folder)
        $to_mailbox_ACLs = $this->_get_acl($to);
        if (!is_array($to_mailbox_ACLs) || !in_array(self::ACL_INSERT_FLAG, $to_mailbox_ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // source folder exists?
        $from_mailbox_idnr = null;
        if ($from != null && !$from_mailbox_idnr = $this->get_mail_box_id($from)) {
            return FALSE;
        }

        // format target message UIDs
        $message_uids = $this->list_message_UIDs($uids, $from);
        if (!$message_uids) {
            return FALSE;
        }

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        // loop message UIDs
        foreach ($message_uids as $message_uid) {

            // Retrieve message record
            $message_metadata = $this->get_message_record($message_uid);
            if (!$message_metadata) {
                // not found
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // ACLs check ('read' grant required )
            $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
            if (!is_array($ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
                // Unauthorized!
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // change mailbox and update 'seq' flag
            $query = "UPDATE dbmail_messages "
                    . " SET mailbox_idnr = {$this->dbmail->escape($to_mailbox_idnr)} "
                    . " WHERE message_idnr = {$this->dbmail->escape($message_uid)}";

            if (!$this->dbmail->query($query)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // increment 'SEQ' flag on src mailbox
            if (!$this->increment_mailbox_seq($from_mailbox_idnr)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // increment 'SEQ' flag on dest mailbox and moved message
            if (!$this->increment_mailbox_seq($to_mailbox_idnr, $message_uid)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        // return status
        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    /**
     * Copy message(s) from one mailbox to another.
     *
     * (physmessage deduplication aware!!!!!)
     * 
     * @param mixed  $uids  Message UIDs as array or comma-separated string, or '*'
     * @param string $to    Target folder
     * @param string $from  Source folder
     *
     * @return boolean True on success, False on error
     */
    public function copy_message($uids, $to, $from = null) {

        // destination folder exists?
        $to_mailbox_idnr = $this->get_mail_box_id($to);
        if (!$to_mailbox_idnr) {
            return FALSE;
        }

        // ACLs check ('insert' grant required for destination folder)
        $to_mailbox_ACLs = $this->_get_acl($to);
        if (!is_array($to_mailbox_ACLs) || !in_array(self::ACL_INSERT_FLAG, $to_mailbox_ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // source folder exists?
        $from_mailbox_idnr = null;
        if ($from != null && !$from_mailbox_idnr = $this->get_mail_box_id($from)) {
            return FALSE;
        }

        // format supplied message UIDs list
        $message_uids = $this->list_message_UIDs($uids, $from);
        if (!$message_uids) {
            return FALSE;
        }

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        // loop messages
        foreach ($message_uids as $message_uid) {

            // Retrieve message record
            $message_metadata = $this->get_message_record($message_uid);
            if (!$message_metadata) {
                // not found
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // ACLs check ('read' grant required )
            $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
            if (!is_array($ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
                // Unauthorized!
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // flag 'deleted' validation
            if ($message_metadata['deleted_flag']) {
                // don't copy deleted messages
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // Retrieve physmessage record
            $physmessage_id = $message_metadata['physmessage_id'];
            $physmessage_metadata = $this->get_physmessage_record($physmessage_id);
            if (!$physmessage_metadata) {
                // not found
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // validate source / destination folders
            if ($message_metadata['mailbox_idnr'] == $to_mailbox_idnr) {
                // source folder equal to destination folder, do nothing!
                continue;
            }

            // save cloned message
            $query = "INSERT INTO dbmail_messages "
                    . " ( "
                    . "    mailbox_idnr, "
                    . "    physmessage_id, "
                    . "    seen_flag, "
                    . "    unique_id, "
                    . "    status "
                    . " ) "
                    . " VALUES "
                    . " ("
                    . "    '{$this->dbmail->escape($to_mailbox_idnr)}', "
                    . "    '{$this->dbmail->escape($physmessage_id)}', "
                    . "    1, "
                    . "    '{$this->dbmail->escape($this->create_message_unique_id($message_uid))}', "
                    . "    '{$this->dbmail->escape(self::MESSAGE_STATUS_SEEN)}' "
                    . " )";

            if (!$this->dbmail->query($query)) {
                // Error while executing query
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // retrieve message ID
            $message_idnr = $this->dbmail->insert_id('dbmail_messages');

            // increment user quota
            if (!$this->increment_user_quota($this->user_idnr, $physmessage_metadata['messagesize'])) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // increment 'SEQ' flag on '$to_mailbox' and '$message'
            if (!$this->increment_mailbox_seq($to_mailbox_idnr, $message_idnr)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // increment 'SEQ' flag on '$from_mailbox'
            if (!$this->increment_mailbox_seq($message_metadata['mailbox_idnr'])) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        // return status
        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    /**
     * Mark message(s) as deleted and expunge.
     *
     * @param mixed   $uids                 Message UIDs as array or comma-separated string, or '*'
     * @param string  $folder               Source folder
     *
     * @return boolean True on success, False on error
     */
    public function delete_message($uids, $folder = NULL) {

        /*
         * DON'T USE TRANSACTIONS WITHIN THIS METHOD!!!!!
         * 
         * TRANSACTIONS MUST BE STARTED INTO CALLER METHOD!!!
         */

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        // format supplied message UIDs list
        $message_uids = $this->list_message_UIDs($uids, $folder);
        if (!$message_uids) {
            // Empty folder - do nothing!
            return TRUE;
        }

        // validate ACLs for each message
        foreach ($message_uids as $message_uid) {

            // Retrieve message record
            $message_metadata = $this->get_message_record($message_uid);
            if (!$message_metadata) {
                return FALSE;
            }

            // ACLs check ('deleted' and 'expunge' grants required )
            $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
            if (!is_array($ACLs) || !in_array(self::ACL_DELETED_FLAG, $ACLs) || !in_array(self::ACL_EXPUNGE_FLAG, $ACLs)) {
                // Unauthorized!
                return FALSE;
            }

            // Set 'deleted' flag
            $query = "UPDATE dbmail_messages "
                    . " SET deleted_flag=1 "
                    . "WHERE message_idnr = {$this->dbmail->escape($message_uid)} ";

            if (!$this->dbmail->query($query)) {
                return FALSE;
            }

            // Update 'SEQ' flag
            if (!$this->increment_mailbox_seq($message_metadata['mailbox_idnr'], $message_uid)) {
                return FALSE;
            }
        }

        return $this->expunge_message($uids, $folder, false);
    }

    /**
     * Expunge message(s) and clear the cache.
     *
     * @param mixed   $uids        Message UIDs as array or comma-separated string, or '*'
     * @param string  $folder      Folder name
     * @param boolean $clear_cache False if cache should not be cleared
     *
     * @return boolean True on success, False on error
     */
    public function expunge_message($uids, $folder = null, $clear_cache = true) {

        /*
         * DON'T USE TRANSACTIONS WITHIN THIS METHOD!!!!!
         * 
         * TRANSACTIONS MUST BE STARTED INTO CALLER METHOD!!!
         */

        list($uids, $all_mode) = $this->parse_uids($uids);

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        // folder exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        /*
         * Expunge ALL the deleted mails in a folder
         */
        if (empty($uids) || $all_mode) {

            // Expunge all!
            $result = $this->expungeAll($mailbox_idnr); // da rinominare

            return ($result == FALSE ? FALSE : TRUE);
        } else {

            /*
             * Expunge SOME mails from a folder
             * 
             * format supplied message UIDs list - THIRD PARAMETER enable deleted_flag check
             */
            $message_uids = $this->list_message_UIDs($uids, $folder, TRUE);
            if (!$message_uids) {
                // Empty folder - do nothing!
                return TRUE;
            }

            foreach ($message_uids as $message_uid) {

                // Retrieve message record
                $message_metadata = $this->get_message_record($message_uid);
                if (!$message_metadata) {
                    // not found
                    return FALSE;
                }

                // ACLs check ('deleted' and 'expunge' grants required )
                $ACLs = $this->_get_acl(NULL, $message_metadata['mailbox_idnr']);
                if (!is_array($ACLs) || !in_array(self::ACL_DELETED_FLAG, $ACLs) || !in_array(self::ACL_EXPUNGE_FLAG, $ACLs)) {
                    // Unauthorized!
                    return FALSE;
                }

                // Retrieve physmessage record
                $physmessage_metadata = $this->get_physmessage_record($message_metadata['physmessage_id']);
                if (!$physmessage_metadata) {
                    // not found
                    return FALSE;
                }

                $query = "UPDATE dbmail_messages "
                        . "SET status = 2 "
                        . "WHERE  message_idnr = {$this->dbmail->escape($message_uid)} "
                        . "and deleted_flag = 1";

                if (!$this->dbmail->query($query)) {
                    // rollbalk transaction
                    return FALSE;
                }

                // decrement user quota
                if (!$this->decrement_user_quota($this->user_idnr, $physmessage_metadata['messagesize'])) {
                    return FALSE;
                }
            }

            if (!$this->increment_mailbox_seq($mailbox_idnr)) {
                return FALSE;
            }

            return TRUE;
        }
    }

    /* --------------------------------
     *        folder managment
     * -------------------------------- */

    /**
     * Get a list of subscribed folders.
     *
     * @param   string  $root      Optional root folder
     * @param   string  $name      Optional name pattern
     * @param   string  $filter    Optional filter
     * @param   string  $rights    Optional ACL requirements
     * @param   bool    $skip_sort Enable to return unsorted list (for better performance)
     *
     * @return  array   List of folders
     */
    public function list_folders_subscribed($root = '', $name = '*', $filter = null, $rights = null, $skip_sort = false, $subscribed = true) {

        return $this->list_folders($root, $name, $filter, $rights, $skip_sort, $subscribed);
    }

    /**
     * Method for direct folders listing (LSUB)
     *
     * @param string $root Optional root folder
     * @param string $name Optional name pattern
     *
     * @return array List of subscribed folders
     * @see rcube_imap::list_folders_subscribed()
     */
    public function list_folders_subscribed_direct($root = '', $name = '*') {

        /**
         * retrieve root folder ID
         */
        $root_folder_idnr = $this->get_mail_box_id($root);
        if (!$root_folder_idnr) {
            return array();
        }

        /**
         * Init folders list container
         */
        $folders = array();

        /**
         * Append supplied root folder
         */
        $folders[$root_folder_idnr] = $root;

        /**
         * Append sub-folders
         */
        foreach ($this->get_sub_folders($root) as $sub_folder_idnr => $sub_folder_name) {
            $folders[$sub_folder_idnr] = $sub_folder_name;
        }

        /**
         * Init subscribed folders list
         */
        $subscribed_folders = array();

        /**
         * Fetch folders list to keep subscribed folders only
         */
        foreach ($folders as $folder_idnr => $folder_name) {

            if ($this->folder_subscription_exists($folder_idnr) && !in_array($folder_name, $subscribed_folders)) {

                $subscribed_folders[] = $folder_name;
            }
        }

        return $subscribed_folders;
    }

    /**
     * Get a list of all folders available on the server.
     *
     * @param string  $root      IMAP root dir
     * @param string  $name      Optional name pattern
     * @param mixed   $filter    Optional filter
     * @param string  $rights    Optional ACL requirements
     * @param bool    $skip_sort Enable to return unsorted list (for better performance)
     * @param boolean $subscribed Return only subscribed folders
     *
     * @return array Indexed array with folder names
     */
    public function list_folders($root = '', $name = '*', $filter = null, $rights = null, $skip_sort = false, $subscribed = false) {

        // get 'user' forlders
        $query = "SELECT name FROM dbmail_mailboxes ";

        if ($subscribed) {
            $query .= "inner join dbmail_subscription ON mailbox_idnr = mailbox_id ";
        }

        $query .= " WHERE owner_idnr = {$this->dbmail->escape($this->user_idnr)} "
                . " AND deleted_flag = 0 ";

        if (!$skip_sort) {
            $query .= " ORDER BY name ASC ";
        }

        $res = $this->dbmail->query($query);

        $folders = array();
        $additionalFolders = array();
        while ($row = $this->dbmail->fetch_assoc($res)) {

            if (strtoupper(substr($row['name'], 0, 5)) == 'INBOX') {
                // add 'INBOX' to main folders list
                $folders[] = $row['name'];
            } else {
                // put everything except 'INBOX' within '$additionalFolders' container
                $additionalFolders[] = $row['name'];
            }
        }

        // append $additionalFolders to main folders list
        foreach ($additionalFolders as $additionalFolder) {
            $folders[] = $additionalFolder;
        }

        // search for shared folders
        $sharingsUserPath = $this->namespace['other'][0][0];
        $sharingsUserDelimiter = $this->namespace['other'][0][1];
        $sharingsPublicPath = $this->namespace['shared'][0][0];
        $sharingsPublicDelimiter = $this->namespace['shared'][0][1];

        $sharingsSql = "SELECT dbmail_users.userid AS mailbox_owner, dbmail_mailboxes.mailbox_idnr, dbmail_mailboxes.name as mailbox_name "
                . "FROM dbmail_acl "
                . "INNER JOIN dbmail_subscription ON dbmail_acl.user_id = dbmail_subscription.user_id AND dbmail_acl.mailbox_id = dbmail_subscription.mailbox_id "
                . "INNER JOIN dbmail_mailboxes ON dbmail_acl.mailbox_id = dbmail_mailboxes.mailbox_idnr "
                . "INNER JOIN dbmail_users ON dbmail_mailboxes.owner_idnr = dbmail_users.user_idnr "
                . "WHERE dbmail_acl.user_id = {$this->dbmail->escape($this->user_idnr)} "
                . "AND dbmail_acl.lookup_flag = 1 "
                . "ORDER BY mailbox_owner, mailbox_name ";

        $sharingsResult = $this->dbmail->query($sharingsSql);
        while ($sharing = $this->dbmail->fetch_assoc($sharingsResult)) {

            $mailbox_owner = $sharing['mailbox_owner'];
            $mailbox_name = $sharing['mailbox_name'];

            // folder owner by 'PUBLIC_FOLDER_USER' or by a real user?
            if ($mailbox_owner == self::PUBLIC_FOLDER_USER) {
                // hide owner userId from folder path
                $path = $sharingsPublicPath . $sharingsPublicDelimiter . $mailbox_name;
            } else {
                $path = $sharingsUserPath . $sharingsUserDelimiter . $mailbox_owner . $sharingsUserDelimiter . $mailbox_name;
            }

            $folders[] = $path;
        }

        return $folders;
    }

    /**
     * Subscribe to a specific folder(s)
     *
     * @param array $folders Folder name(s)
     *
     * @return boolean True on success
     */
    public function subscribe($folders) {

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        foreach ($folders as $folder) {

            // target folder exists?
            $mailbox_idnr = $this->get_mail_box_id($folder);
            if (!$mailbox_idnr) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            /*
             * Insert / Update dbmail_subscription entry
             */
            $sql = "INSERT INTO dbmail_subscription "
                    . "(user_id, mailbox_id) "
                    . "VALUES "
                    . "('{$this->dbmail->escape($this->user_idnr)}', '{$this->dbmail->escape($mailbox_idnr)}' ) "
                    . "ON DUPLICATE KEY UPDATE "
                    . "user_id = '{$this->dbmail->escape($this->user_idnr)}', "
                    . "mailbox_id = '{$this->dbmail->escape($mailbox_idnr)}' ";

            if (!$this->dbmail->query($sql)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        // return status
        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    /**
     * Unsubscribe folder(s)
     *
     * @param array $folders Folder name(s)
     *
     * @return boolean True on success
     */
    public function unsubscribe($folders) {

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        foreach ($folders as $folder) {

            // target folder exists?
            $mailbox_idnr = $this->get_mail_box_id($folder);
            if (!$mailbox_idnr) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            $query = " DELETE FROM dbmail_subscription "
                    . "WHERE user_id = {$this->dbmail->escape($this->user_idnr)} "
                    . "AND mailbox_id = {$this->dbmail->escape($mailbox_idnr)}";

            if (!$this->dbmail->query($query)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        // return status
        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    /**
     * Create a new folder on the server.
     *
     * @param string  $folder    New folder name
     * @param boolean $subscribe True if the newvfolder should be subscribed
     *
     * @return boolean True on success, False on error
     */
    public function create_folder($folder, $subscribe = false) {

        if (strlen($folder) == 0) {
            // empty folder name!
            return FALSE;
        }

        // get parent folder (if any) to perform ACLs checks
        $exploded = explode($this->delimiter, $folder);
        if (is_array($exploded) && count($exploded) > 1) {

            // folder is not whithin root level, check parent grants
            $parentFolder = implode($this->delimiter, array_slice($exploded, 0, -1));

            // ACLs check ('create' grant required )
            $ACLs = $this->_get_acl($parentFolder);
            if (!is_array($ACLs) || !in_array(self::ACL_CREATE_FLAG, $ACLs)) {
                // Unauthorized!
                return FALSE;
            }
        }

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        // prepare query
        $mailboxSQL = " INSERT INTO dbmail_mailboxes "
                . " ("
                . "     owner_idnr, "
                . "     name, "
                . "     seen_flag, "
                . "     answered_flag, "
                . "     deleted_flag, "
                . "     flagged_flag, "
                . "     recent_flag, "
                . "     draft_flag, "
                . "     no_inferiors, "
                . "     no_select, "
                . "     permission, "
                . "     seq"
                . " ) "
                . " VALUES "
                . " ("
                . "     {$this->user_idnr}, "
                . "     '{$this->dbmail->escape($folder)}', "
                . "     0, "
                . "     0, "
                . "     0, "
                . "     0, "
                . "     0, "
                . "     0, "
                . "     0, "
                . "     0, "
                . "     2, "
                . "     0 "
                . " ) ";

        // insert new folder record
        if (!$this->dbmail->query($mailboxSQL)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // retreive last insert id
        $mailbox_idnr = $this->dbmail->insert_id('dbmail_mailboxes');

        // subscription management (if needed)
        if ($subscribe) {

            /*
             * Insert / Update dbmail_subscription entry
             */
            $subscriptionSQL = "INSERT INTO dbmail_subscription "
                    . "(user_id, mailbox_id) "
                    . "VALUES "
                    . "('{$this->dbmail->escape($this->user_idnr)}', '{$this->dbmail->escape($mailbox_idnr)}' ) "
                    . "ON DUPLICATE KEY UPDATE "
                    . "user_id = '{$this->dbmail->escape($this->user_idnr)}', "
                    . "mailbox_id = '{$this->dbmail->escape($mailbox_idnr)}' ";

            if (!$this->dbmail->query($subscriptionSQL)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        // return status
        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    /**
     * Set a new name to an existing folder
     *
     * @param string $folder   Folder to rename
     * @param string $new_name New folder name
     *
     * @return boolean True on success, False on error
     */
    public function rename_folder($folder, $new_name) {

        // mailbox esists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            // mailbox not found
            return FALSE;
        }

        // ACLs check ('create' grant required )
        $ACLs = $this->_get_acl($folder);
        if (!is_array($ACLs) || !in_array(self::ACL_CREATE_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // new mailbox name already exists?
        if ($this->get_mail_box_id($new_name)) {
            // name already exist
            return FALSE;
        }

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        // has children?
        $sub_folders = $this->get_sub_folders($folder);

        if (count($sub_folders) > 0) {

            // target path segment level
            $current_path_segment_level = count(explode($this->delimiter, $folder));

            // fetch children
            foreach ($sub_folders as $sub_folder_idnr => $sub_folder_name) {

                // explode sub folder name
                $exploded_sub_folder = explode($this->delimiter, $sub_folder_name);

                // append to $new_name sub folders
                $new_sub_folder_name = $new_name . $this->delimiter . implode($this->delimiter, array_slice($exploded_sub_folder, $current_path_segment_level));

                // rename sub folder
                $query = "UPDATE dbmail_mailboxes "
                        . " set name = '{$this->dbmail->escape($new_sub_folder_name)}' "
                        . " WHERE mailbox_idnr = {$this->dbmail->escape($sub_folder_idnr)} ";

                if (!$this->dbmail->query($query)) {
                    // rollbalk transaction
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }

                // increment folder 'seq' flag
                if (!$this->increment_mailbox_seq($sub_folder_idnr)) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }

                // unset temporary stored mailbox_id (if present)
                if (!$this->unset_temp_value("MBOX_ID_{$sub_folder_name}_{$this->user_idnr}")) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }
            }
        }

        // rename target folder
        $query = "UPDATE dbmail_mailboxes "
                . " set name = '{$this->dbmail->escape($new_name)}' "
                . " WHERE mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

        if (!$this->dbmail->query($query)) {
            // rollbalk transaction
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // increment folder 'seq' flag
        if (!$this->increment_mailbox_seq($mailbox_idnr)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // unset temporary stored mailbox_id (if present)
        if (!$this->unset_temp_value("MBOX_ID_{$folder}_{$this->user_idnr}")) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    public function count_message_folder($folder) {

        /* serve perchè altrimenti non cancella una cartella vuota */
        $mailbox_idnr = $this->get_mail_box_id($folder);
        $query = "SELECT message_idnr "
                . " FROM dbmail_messages "
                . " WHERE mailbox_idnr = '{$this->dbmail->escape($mailbox_idnr)}'";

        $res = $this->dbmail->query($query);

        /* fine */
        return $this->dbmail->num_rows($res);
    }

    /**
     * Remove a folder from the server.
     *
     * @param string $folder Folder name
     *
     * @return boolean True on success, False on error
     */
    public function delete_folder($folder) {

        // mailbox esists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            // mailbox not found
            return FALSE;
        }

        // ACLs check ('delete' grant required )
        $ACLs = $this->_get_acl(NULL, $mailbox_idnr);
        if (!is_array($ACLs) || !in_array(self::ACL_DELETE_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // init folders container
        $folders_list = array(
            $mailbox_idnr => $folder
        );

        // get mailbox sub folders
        $sub_folders_list = $this->get_sub_folders($folder);

        // merge sub folders with target folder
        if (is_array($sub_folders_list)) {

            // loop sub folders
            foreach ($sub_folders_list as $sub_folder_idnr => $sub_folder_name) {

                // ACLs check ('delete' grant required )
                $ACLs = $this->_get_acl(NULL, $sub_folder_idnr);

                if (!is_array($ACLs) || !in_array(self::ACL_DELETE_FLAG, $ACLs)) {
                    // Unauthorized!
                    return FALSE;
                }

                // add sub folder to folders list
                $folders_list[$sub_folder_idnr] = $sub_folder_name;
            }
        }

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        // delete folders 
        foreach ($folders_list as $folder_idnr => $folder_name) {

            // delete messages
            if (!$this->clear_folder($folder_name)) {
                // rollbalk transaction
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // delete folder
            $query = "DELETE FROM dbmail_mailboxes "
                    . " WHERE mailbox_idnr = {$this->dbmail->escape($folder_idnr)} ";

            if (!$this->dbmail->query($query)) {
                // rollbalk transaction
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // unset temporary stored mailbox_id (if present)
            if (!$this->unset_temp_value("MBOX_ID_{$folder_name}_{$this->user_idnr}")) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    /**
     * Send expunge command and clear the cache.
     *
     * @param string  $folder      Folder name
     * @param boolean $clear_cache False if cache should not be cleared
     *
     * @return boolean True on success, False on error
     */
    public function expunge_folder($folder = null, $clear_cache = true) {
        return $this->expunge_message('*', $folder, $clear_cache);
    }

    /**
     * Remove all messages in a folder..
     *
     * @param string  $folder  Folder name
     *
     * @return boolean True on success, False on error
     */
    public function clear_folder($folder = null) {
        return $this->delete_message('*', $folder);
    }

    /**
     * Checks if folder exists and is subscribed
     *
     * @param string   $folder       Folder name
     * @param boolean  $subscription Enable subscription checking
     *
     * @return boolean True if folder exists, False otherwise
     */
    public function folder_exists($folder, $subscription = false) {

        // folder exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        if ($subscription && !$this->folder_subscription_exists($mailbox_idnr)) {
            return FALSE;
        }

        return TRUE;
    }

    /**
     * Get folder size (size of all messages in a folder)
     *
     * @param string $folder Folder name
     *
     * @return int Folder size in bytes, False on error
     */
    public function folder_size($folder) {

        $mailbox_idnr = $this->get_mail_box_id($folder);

        $query = " SELECT SUM(dbmail_physmessage.messagesize) as folder_size "
                . " FROM dbmail_messages "
                . " INNER JOIN dbmail_physmessage on dbmail_messages.physmessage_id = dbmail_physmessage.id "
                . " WHERE dbmail_messages.mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} "
                . " AND dbmail_messages.deleted_flag = 0 ";

        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        return (is_array($row) && array_key_exists('folder_size', $row) ? $row['folder_size'] : 0);
    }

    /**
     * Returns the namespace where the folder is in
     *
     * @param string $folder Folder name
     *
     * @return string One of 'personal', 'other' or 'shared'
     */
    public function folder_namespace($folder) {

        if ($folder == 'INBOX') {
            return 'personal';
        }

        foreach ($this->namespace as $type => $namespace) {
            if (is_array($namespace)) {
                foreach ($namespace as $ns) {
                    if ($len = strlen($ns[0])) {
                        if (($len > 1 && $folder == substr($ns[0], 0, -1)) || strpos($folder, $ns[0]) === 0
                        ) {
                            return $type;
                        }
                    }
                }
            }
        }

        return 'personal';
    }

    /**
     * Gets folder attributes (from LIST response, e.g. \Noselect, \Noinferiors).
     *
     * @param string $folder  Folder name
     * @param bool   $force   Set to True if attributes should be refreshed
     *
     * @return array Options list
     */
    public function folder_attributes($folder, $force = false) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        // folder exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            // not found
            return FALSE;
        }

        // Retrieve folder record
        $mailbox_metadata = $this->get_folder_record($mailbox_idnr);
        if (!$mailbox_metadata) {
            // not found
            return FALSE;
        }

        return array(
            'seen' => $mailbox_metadata['seen_flag'],
            'answered' => $mailbox_metadata['answered_flag'],
            'deleted' => $mailbox_metadata['deleted_flag'],
            'flagged' => $mailbox_metadata['flagged_flag'],
            'recent' => $mailbox_metadata['recent_flag'],
            'draft' => $mailbox_metadata['draft_flag'],
            '\\Noselect' => $mailbox_metadata['no_select']
        );
    }

    /**
     * Gets connection (and current folder) data: UIDVALIDITY, EXISTS, RECENT,
     * PERMANENTFLAGS, UIDNEXT, UNSEEN
     *
     * @param string $folder Folder name
     *
     * @return array Data
     */
    public function folder_data($folder) {
        // TO DO!!!!!!
    }

    /**
     * Returns extended information about the folder.
     *
     * @param string $folder Folder name
     *
     * @return array Data
     */
    public function folder_info($folder) {

        /*
         *  folder name could be too long to be used as cache key, so we hash it 
         * to prevent "Data too long for column 'cache_key'" issues
         */
        $cache_key = "FOLDER_" . md5($folder);

        $folder_cached = $this->get_cache($cache_key);
        if (is_object($folder_cached)) {
            return $folder_cached;
        }

        $folderAttributes = $this->folder_attributes($folder);
        $folderRights = $this->_get_acl($folder);

        $options = array(
            'is_root' => FALSE,
            'name' => $folder,
            'attributes' => $folderAttributes,
            'namespace' => $this->folder_namespace($folder),
            'special' => (in_array($folder, self::$folder_types) ? TRUE : FALSE),
            'noselect' => (array_key_exists('no_select', $folderAttributes) ? $folderAttributes['no_select'] : FALSE),
            'rights' => $folderRights,
            'norename' => (in_array(self::ACL_DELETE_FLAG, $folderRights) ? FALSE : TRUE)
        );

        $this->update_cache($cache_key, $options);

        return $options;
    }

    /**
     * Returns current status of a folder (compared to the last time use)
     *
     * @param string $folder Folder name
     * @param array  $diff   Difference data
     *
     * @return int Folder status
     */
    public function folder_status($folder = null, &$diff = array()) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }
        $old = $this->get_folder_stats($folder);

        $result = 0;

        if (empty($old)) {
            return $result;
        }

        // refresh message count -> will update
        $this->count($folder, 'ALL', FALSE, TRUE);

        $new = $this->get_folder_stats($folder);

        // got new messages
        if ($new['maxuid'] > $old['maxuid']) {
            $result += 1;
            // get new message UIDs range, that can be used for example
            // to get the data of these messages
            $diff['new'] = ($old['maxuid'] + 1 < $new['maxuid'] ? ($old['maxuid'] + 1) . ':' : '') . $new['maxuid'];
        }
        // some messages has been deleted
        if ($new['cnt'] < $old['cnt']) {
            $result += 2;
        }

        // @TODO: optional checking for messages flags changes (?)
        // @TODO: UIDVALIDITY checking

        return $result;
    }

    /**
     * Synchronizes messages cache.
     *
     * @param string $folder Folder name
     */
    public function folder_sync($folder) {



        // TO DO!!!!!
    }

    /**
     * Modify folder name according to namespace.
     * For output it removes prefix of the personal namespace if it's possible.
     * For input it adds the prefix. Use it before creating a folder in root
     * of the folders tree.
     *
     * @param string $folder  Folder name
     * @param string $mode    Mode name (out/in)
     *
     * @return string Folder name
     */
    public function mod_folder($folder, $mode = 'out') {

        if (strlen($folder) == 0) {
            return $folder;
        }

        $prefix = $this->namespace['prefix']; // see set_env()
        $prefix_len = strlen($prefix);

        if (!$prefix_len) {
            return $folder;
        }

        // remove prefix for output
        if ($mode == 'out') {
            if (substr($folder, 0, $prefix_len) === $prefix) {
                return substr($folder, $prefix_len);
            }
        }
        // add prefix for input (e.g. folder creation)
        else {
            return $prefix . $folder;
        }

        return $folder;
    }

    /**
     * Create all folders specified as default
     */
    public function create_default_folders() {
        $rcube = rcube::get_instance();

        // create default folders if they do not exist
        foreach (self::$folder_types as $type) {
            if ($folder = $rcube->config->get($type . '_mbox')) {
                if (!$this->folder_exists($folder)) {
                    $this->create_folder($folder, true, $type);
                } else if (!$this->folder_exists($folder, true)) {
                    $this->subscribe($folder);
                }
            }
        }
    }

    /**
     * Check if specified folder is a special folder
     */
    public function is_special_folder($name) {
        return $name == 'INBOX' || in_array($name, $this->get_special_folders());
    }

    /**
     * Return configured special folders
     */
    public function get_special_folders($forced = false) {
        // getting config might be expensive, store special folders in memory
        /*  if (!isset($this->icache['special-folders'])) {
          $rcube = rcube::get_instance();
          $this->icache['special-folders'] = array();

          foreach (self::$folder_types as $type) {
          if ($folder = $rcube->config->get($type . '_mbox')) {
          $this->icache['special-folders'][$type] = $folder;
          }
          }
          }

          return $this->icache['special-folders']; */
        return array();
    }

    /**
     * Set special folder associations stored in backend
     */
    public function set_special_folders($specials) {
        // should be overriden by storage class if backend supports special folders (SPECIAL-USE)
        // unset($this->icache['special-folders']);
    }

    /**
     * Get mailbox quota information.
     *
     * @param string $folder  Folder name
     *
     * @return mixed Quota info or False if not supported
     */
    public function get_quota($folder = null) {

        $query = "SELECT curmail_size, maxmail_size, cursieve_size, maxsieve_size"
                . "  FROM dbmail_users "
                . " WHERE user_idnr = {$this->dbmail->escape($this->user_idnr)} ";

        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        $used = intval($row["curmail_size"]);
        $total = intval($row["maxmail_size"]);
        $sieveused = intval($row["cursieve_size"]);
        $sievetotal = intval($row["maxsieve_size"]);

        $result['used'] = $used;
        $result['total'] = $total;
        $result['percent'] = min(100, round(($used / max(1, $total)) * 100));
        $result['free'] = 100 - $result['percent'];

        /*
         * This is creative hack to show both configurable infos for dbmail.
         * 
         * NOTE!!!!!!
         * 
         * program/include/rcmail.php (row 1761) takes those values and multiply them by 1024!!!!!!!
         */
        $result['all']["Messages"]["storage"]["used"] = round($used / 1024);
        $result['all']["Messages"]["storage"]["total"] = round($total / 1024);
        $result['all']["Rules"]["storage"]["used"] = round($sieveused / 1024);
        $result['all']["Rules"]["storage"]["total"] = round($sievetotal / 1024);

        return $result;
    }

    /* -----------------------------------------
     *   ACL and METADATA methods
     * ---------------------------------------- */

    /**
     * Changes the ACL on the specified folder (SETACL)
     *
     * @param string $folder  Folder name
     * @param string $user    User name
     * @param string $acl     ACL string
     *
     * @return boolean True on success, False on failure
     */
    public function set_acl($folder, $user, $acl) {

        /*
         * Get mailbox ID
         */
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        /*
         * Get user ID
         */
        $user_idnr = $this->get_user_id($user);
        if (!$user_idnr) {
            return FALSE;
        }

        /*
         *  split ACLs string to array
         */
        $exploded_acls = str_split($acl);

        /*
         * Insert / Update dbmail_acl entry
         */
        $ACLsSQL = "INSERT INTO dbmail_acl "
                . " ("
                . "     user_id,"
                . "     mailbox_id, "
                . "     lookup_flag, "
                . "     read_flag, "
                . "     seen_flag, "
                . "     write_flag, "
                . "     insert_flag, "
                . "     post_flag, "
                . "     create_flag, "
                . "     delete_flag, "
                . "     administer_flag "
                . " )"
                . " VALUES "
                . " ("
                . "     {$this->dbmail->escape($user_idnr)}, "
                . "     {$this->dbmail->escape($mailbox_idnr)}, "
                . " " . (in_array('l', $exploded_acls) ? 1 : 0) . ", "
                . " " . (in_array('r', $exploded_acls) ? 1 : 0) . ", "
                . " " . (in_array('s', $exploded_acls) ? 1 : 0) . ", "
                . " " . (in_array('w', $exploded_acls) ? 1 : 0) . ", "
                . " " . (in_array('i', $exploded_acls) ? 1 : 0) . ", "
                . " " . (in_array('p', $exploded_acls) ? 1 : 0) . ", "
                . " " . (in_array('c', $exploded_acls) ? 1 : 0) . ", "
                . " " . (in_array('d', $exploded_acls) ? 1 : 0) . ", "
                . " " . (in_array('a', $exploded_acls) ? 1 : 0) . " "
                . " ) "
                . "ON DUPLICATE KEY UPDATE "
                . "lookup_flag = " . (in_array('l', $exploded_acls) ? 1 : 0) . ", "
                . "read_flag = " . (in_array('r', $exploded_acls) ? 1 : 0) . ", "
                . "seen_flag = " . (in_array('s', $exploded_acls) ? 1 : 0) . ", "
                . "write_flag = " . (in_array('w', $exploded_acls) ? 1 : 0) . ", "
                . "insert_flag = " . (in_array('i', $exploded_acls) ? 1 : 0) . ", "
                . "post_flag = " . (in_array('p', $exploded_acls) ? 1 : 0) . ", "
                . "create_flag = " . (in_array('c', $exploded_acls) ? 1 : 0) . ", "
                . "delete_flag = " . (in_array('d', $exploded_acls) ? 1 : 0) . ", "
                . "administer_flag = " . (in_array('a', $exploded_acls) ? 1 : 0);


        $this->dbmail->query($ACLsSQL);

        /*
         * Insert / Update dbmail_subscription entry
         */
        $subscriptionSQL = "INSERT INTO dbmail_subscription "
                . "(user_id, mailbox_id) "
                . "VALUES "
                . "('{$this->dbmail->escape($user_idnr)}', '{$this->dbmail->escape($mailbox_idnr)}' ) "
                . "ON DUPLICATE KEY UPDATE "
                . "user_id = '{$this->dbmail->escape($user_idnr)}', "
                . "mailbox_id = '{$this->dbmail->escape($mailbox_idnr)}' ";

        $this->dbmail->query($subscriptionSQL);

        return TRUE;
    }

    /**
     * Removes any <identifier,rights> pair for the
     * specified user from the ACL for the specified
     * folder (DELETEACL).
     *
     * @param string $folder  Folder name
     * @param string $user    User name
     *
     * @return boolean True on success, False on failure
     */
    public function delete_acl($folder, $user) {

        /*
         * Get mailbox ID
         */
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        /*
         * Get user ID
         */
        $user_idnr = $this->get_user_id($user);
        if (!$user_idnr) {
            return FALSE;
        }

        /*
         * Delete ACl entry
         */
        $query = "DELETE FROM dbmail_acl"
                . " WHERE user_id = '{$this->dbmail->escape($mailbox_idnr)}' "
                . " AND mailbox_id = '{$this->dbmail->escape($user_idnr)}' ";

        $this->dbmail->query($query);

        /*
         * Delete subscription entry
         */
        $query = "DELETE FROM dbmail_subscription"
                . " WHERE user_id = '{$this->dbmail->escape($mailbox_idnr)}' "
                . " AND mailbox_id = '{$this->dbmail->escape($user_idnr)}' ";

        $this->dbmail->query($query);

        return TRUE;
    }

    /**
     * Returns the access control list for a folder (GETACL).
     *
     * @param string $folder Folder name
     *
     * @return array User-rights array on success, NULL on error
     */
    public function get_acl($folder) {

        return $this->_get_acl($folder);
    }

    /**
     * Wrapped get_acl() method to accept even $folderName or $folderId as target folder or a custom userID instead of current userId
     * @param string    $folderName    mailbox name
     * @param int       $folderId      mailbox id
     * @param int       $userId        target user id
     */
    private function _get_acl($folderName = NULL, $folderId = NULL, $userId = NULL) {

        /**
         * Cached entry exists?
         */
        $temp_value_key = md5("ACL_{$this->user_idnr}_{$folderName}_{$folderId}_{$userId}");

        $temp_content = $this->get_temp_value($temp_value_key);
        if ($temp_content && is_array($temp_content)) {
            /*
             * Return cached ACLs
             */
            return $temp_content;
        }

        /**
         * ACL map (ref. https://www.ietf.org/rfc/rfc4314.txt)
         * 
         * l - lookup (mailbox is visible to LIST/LSUB commands, SUBSCRIBE mailbox)
         * r - read (SELECT the mailbox, perform STATUS)
         * s - keep seen/unseen information across sessions (set or clear \SEEN flag via STORE, also set \SEEN during APPEND/COPY/ FETCH BODY[...])
         * w - write (set or clear flags other than \SEEN and \DELETED via STORE, also set them during APPEND/COPY)
         * i - insert (perform APPEND, COPY into mailbox)
         * p - post (send mail to submission address for mailbox, not enforced by IMAP4 itself)
         * k - create mailboxes (CREATE new sub-mailboxes in any implementation-defined hierarchy, parent mailbox for the new mailbox name in RENAME)
         * x - delete mailbox (DELETE mailbox, old mailbox name in RENAME)
         * t - delete messages (set or clear \DELETED flag via STORE, set \DELETED flag during APPEND/COPY)
         * e - perform EXPUNGE and expunge as a part of CLOSE
         * a - administer (perform SETACL/DELETEACL/GETACL/LISTRIGHTS)
         * 
         * 'dbmail_acl' table relations:
         * 
         * l - lookup_flag
         * r - read_flag
         * s - seen_flag
         * w - write_flag
         * i - insert_flag
         * p - post_flag
         * k - create_flag
         * x - delete_flag
         * t - deleted_flag
         * e - expunge_flag
         * a - administer_flag
         */
        /*
         * Get target user ID
         */
        $user_idnr = FALSE;
        if (strlen($userId) > 0) {
            // user id supplied
            $user_idnr = $userId;
        } else {
            // use current user id
            $user_idnr = $this->user_idnr;
        }

        if (!$user_idnr) {
            /*
             * Not found!
             */
            return NULL;
        }

        /*
         *  get mailboxID?
         */
        $mailbox_idnr = FALSE;
        if (strlen($folderName) > 0 && strlen($folderId) == 0) {
            // folder name supplied
            $mailbox_idnr = $this->get_mail_box_id($folderName, $user_idnr);
        } elseif (strlen($folderId) > 0) {
            // folder id supplied
            $mailbox_idnr = $folderId;
        }

        if (!$mailbox_idnr) {
            /*
             * Not found!
             */
            return NULL;
        }

        /*
         * Owned mailbox?
         */
        $ownedMailboxSQL = "SELECT * "
                . "FROM dbmail_mailboxes "
                . "WHERE mailbox_idnr = '{$this->dbmail->escape($mailbox_idnr)}' "
                . "AND owner_idnr = '{$this->dbmail->escape($user_idnr)}' ";

        $ownedMailboxResult = $this->dbmail->query($ownedMailboxSQL);

        if ($this->dbmail->num_rows($ownedMailboxResult) == 1) {

            /*
             * Owned mailbox - return full ACLs list
             */
            $grants = array(
                self::ACL_LOOKUP_FLAG,
                self::ACL_READ_FLAG,
                self::ACL_SEEN_FLAG,
                self::ACL_WRITE_FLAG,
                self::ACL_INSERT_FLAG,
                self::ACL_POST_FLAG,
                self::ACL_CREATE_FLAG,
                self::ACL_DELETE_FLAG,
                self::ACL_DELETED_FLAG,
                self::ACL_EXPUNGE_FLAG,
                self::ACL_ADMINISTER_FLAG
            );

            /*
             * Cache ACLs
             */
            $this->set_temp_value($temp_value_key, $grants);

            return $grants;
        }

        /*
         * Shared mailbox?
         */
        $sharedMailboxACLsSQL = "SELECT * "
                . "FROM dbmail_acl "
                . "WHERE user_id = '{$this->dbmail->escape($user_idnr)}' "
                . "AND mailbox_id = '{$this->dbmail->escape($mailbox_idnr)}' ";

        $sharedMailboxACLsResult = $this->dbmail->query($sharedMailboxACLsSQL);

        if ($this->dbmail->num_rows($sharedMailboxACLsResult) == 0) {
            /*
             * Not found!
             */
            return NULL;
        }

        $sharedMailboxACLs = $this->dbmail->fetch_assoc($sharedMailboxACLsResult);

        $ACLs = array();

        if ($sharedMailboxACLs['lookup_flag'] == 1) {
            $ACLs[] = self::ACL_LOOKUP_FLAG;
        }

        if ($sharedMailboxACLs['read_flag'] == 1) {
            $ACLs[] = self::ACL_READ_FLAG;
        }

        if ($sharedMailboxACLs['seen_flag'] == 1) {
            $ACLs[] = self::ACL_SEEN_FLAG;
        }

        if ($sharedMailboxACLs['write_flag'] == 1) {
            $ACLs[] = self::ACL_WRITE_FLAG;
        }

        if ($sharedMailboxACLs['insert_flag'] == 1) {
            $ACLs[] = self::ACL_INSERT_FLAG;
        }

        if ($sharedMailboxACLs['post_flag'] == 1) {
            $ACLs[] = self::ACL_POST_FLAG;
        }

        if ($sharedMailboxACLs['create_flag'] == 1) {
            $ACLs[] = self::ACL_CREATE_FLAG;
        }

        if ($sharedMailboxACLs['delete_flag'] == 1) {
            $ACLs[] = self::ACL_DELETE_FLAG;
        }

        if ($sharedMailboxACLs['deleted_flag'] == 1) {
            $ACLs[] = self::ACL_DELETED_FLAG;
        }

        if ($sharedMailboxACLs['expunge_flag'] == 1) {
            $ACLs[] = self::ACL_EXPUNGE_FLAG;
        }

        if ($sharedMailboxACLs['administer_flag'] == 1) {
            $ACLs[] = self::ACL_ADMINISTER_FLAG;
        }

        /*
         * Cache ACLs
         */
        $this->set_temp_value($temp_value_key, $grants);

        return $ACLs;
    }

    /**
     * Returns information about what rights can be granted to the
     * user (identifier) in the ACL for the folder (LISTRIGHTS).
     *
     * @param string $folder  Folder name
     * @param string $user    User name
     *
     * @return array List of user rights
     */
    public function list_rights($folder, $user) {

        /*
         * retreive user ID
         */
        $user_idnr = $this->get_user_id($user);
        if (!$user_idnr) {
            // Not found
            return array();
        }

        return $this->_get_acl($folder, NULL, $user_idnr);
    }

    /**
     * Returns the set of rights that the current user has to a folder (MYRIGHTS).
     *
     * @param string $folder Folder name
     *
     * @return array MYRIGHTS response on success, NULL on error
     */
    public function my_rights($folder) {

        return $this->_get_acl($folder);
    }

    /**
     * Sets metadata/annotations (SETMETADATA/SETANNOTATION)
     *
     * @param string $folder  Folder name (empty for server metadata)
     * @param array  $entries Entry-value array (use NULL value as NIL)
     *
     * @return boolean True on success, False on failure
     */
    public function set_metadata($folder, $entries) {
        // TO DO!!!!!
    }

    /**
     * Unsets metadata/annotations (SETMETADATA/SETANNOTATION)
     *
     * @param string $folder  Folder name (empty for server metadata)
     * @param array  $entries Entry names array
     *
     * @return boolean True on success, False on failure
     */
    public function delete_metadata($folder, $entries) {
        // TO DO!!!!!
    }

    /**
     * Returns folder metadata/annotations (GETMETADATA/GETANNOTATION).
     *
     * @param string $folder   Folder name (empty for server metadata)
     * @param array  $entries  Entries
     * @param array  $options  Command options (with MAXSIZE and DEPTH keys)
     * @param bool   $force    Disables cache use
     *
     * @return array Metadata entry-value hash array on success, NULL on error
     */
    public function get_metadata($folder, $entries, $options = array(), $force = false) {
        // TO DO!!!!!
    }

    /* -----------------------------------------
     *   Cache related functions
     * ---------------------------------------- */

    /**
     * Delete outdated cache entries
     */
    public function cache_gc() {
        // TO DO!!!!!
    }

    public function expunge_cache() {
        // TO DO!!!!!
    }

    /*
     * -------------------------------------------------------------
     * HELPER METHODS
     * -------------------------------------------------------------
     */

    /**
     * Connect to dbmail database
     *
     * @return True on success, False on failure
     */
    protected function dbmail_connect() {

        $dsn_dbMail = $this->rcubeInstance->config->get('dbmail_dsn', null);

        $this->dbmail = new rcube_db($dsn_dbMail);

        $debug_queries = ($this->rcubeInstance->config->get('dbmail_sql_debug') ? TRUE : FALSE);

        $this->dbmail->set_debug($debug_queries);

        $this->dbmail->db_connect('r');

        return(!is_null($this->dbmail->is_error()) ? FALSE : TRUE);
    }

    /**
     * Retrieve DB Mail $user_idnr from supplied $username (e-mail address)
     */
    protected function get_dbmail_user_idnr($username = '') {

        $sql = "SELECT user_idnr "
                . "FROM dbmail_users "
                . "WHERE userid = '{$this->dbmail->escape($username)}' ";

        $result = $this->dbmail->query($sql);

        if ($this->dbmail->num_rows($result) != 1) {
            // not found
            return FALSE;
        }

        $user = $this->dbmail->fetch_assoc($result);

        return $user['user_idnr'];
    }

    /**
     * Retrieve mailbox name
     * 
     * @param int $mailbox_idnr
     * 
     * @return string mailbox name on success, False on failure
     */
    protected function get_mail_box_name($mailbox_idnr) {

        $sql = "SELECT name "
                . "FROM dbmail_mailboxes "
                . "WHERE mailbox_idnr = '{$this->dbmail->escape($mailbox_idnr)}'";

        $result = $this->dbmail->query($sql);

        if ($this->dbmail->num_rows($result) == 0) {
            // not found
            return FALSE;
        }

        $mailbox = $this->dbmail->fetch_assoc($result);

        if (is_array($mailbox) && array_key_exists('name', $mailbox) && strlen($mailbox['name'])) {
            // return mailbox name
            return $mailbox['name'];
        } else {
            // invalid result
            return FALSE;
        }
    }

    /**
     * Retrieve mailbox identifier
     *
     * @param string  $folder       folder name
     * @param int     $user_idnr    user identifier
     * @return int mailbox_idnr on success, False on failure
     */
    protected function get_mail_box_id($folder, $user_idnr = NULL) {

        /*
         * Use current user if none supplied!
         */
        if (strlen($user_idnr) == 0) {
            $user_idnr = $this->user_idnr;
        }

        /*
         * Temporary content exists?
         * 
         * TEMPORARY CONTENT MUST BE DELETED WHEN DELETING / RENAMING / ... FOLDERS!!!!!
         */
        $temp_key = "MBOX_ID_{$folder}_{$user_idnr}";
        $temp_mail_box_id = $this->get_temp_value($temp_key);
        if ($temp_mail_box_id) {
            return $temp_mail_box_id;
        }

        /*
         * Owned mailbox?
         */
        $ownedMailboxSQL = " SELECT mailbox_idnr "
                . " FROM dbmail_mailboxes "
                . " WHERE owner_idnr = '{$this->dbmail->escape($user_idnr)}' "
                . " AND deleted_flag = 0 "
                . " AND name = '{$this->dbmail->escape($folder)}' ";

        $ownedMailboxResult = $this->dbmail->query($ownedMailboxSQL);

        if ($this->dbmail->num_rows($ownedMailboxResult) == 1) {

            $ownedMailbox = $this->dbmail->fetch_assoc($ownedMailboxResult);

            if (is_array($ownedMailbox) && array_key_exists('mailbox_idnr', $ownedMailbox) && strlen($ownedMailbox['mailbox_idnr'])) {

                /*
                 * Owned mailbox found!
                 * - return mailbox_idnr
                 */

                $this->set_temp_value($temp_key, $ownedMailbox['mailbox_idnr']);

                return $ownedMailbox['mailbox_idnr'];
            }
        }

        /*
         *  Shared mailbox?
         */
        $folderRealName = FALSE;
        $folderOwner = FALSE;
        $sharingsUserPath = $this->namespace['other'][0][0];
        $sharingsUserDelimiter = $this->namespace['other'][0][1];
        $sharingsPublicPath = $this->namespace['shared'][0][0];
        $sharingsPublicDelimiter = $this->namespace['shared'][0][1];

        if (substr($folder, 0, strlen($sharingsUserPath)) == $sharingsUserPath) {
            /*
             * 'user level' sharing
             * eg. #Users/user@mail.mydomain.it/Common/shared
             */
            $exploded = explode($sharingsUserDelimiter, $folder);
            if (is_array($exploded) && count($exploded) >= 3) {
                /*
                 * Remove leading '$sharingsUserPath' and username to retrieve the real folder name
                 */
                $folderRealName = implode($sharingsUserDelimiter, array_slice($exploded, 2));
                $folderOwner = $exploded[1];
            }
        } elseif (substr($folder, 0, strlen($sharingsPublicPath)) == $sharingsPublicPath) {
            /*
             * 'public level' sharing
             * eg. #Public/shared
             */

            $exploded = explode($sharingsPublicDelimiter, $folder);
            if (is_array($exploded) && count($exploded) >= 2) {
                /*
                 * Remove leading '$sharingsPublicPath' to retrieve the real folder name
                 */
                $folderRealName = implode($sharingsPublicDelimiter, array_slice($exploded, 1));
                $folderOwner = self::PUBLIC_FOLDER_USER;
            }
        }

        /*
         * Shared mailbox found?
         */
        if (!$folderRealName || !$folderOwner) {
            /*
             * Not found!
             */
            return FALSE;
        }

        /*
         * Get mailbox ID
         */
        $sharedMailboxSQL = "SELECT dbmail_mailboxes.mailbox_idnr "
                . "FROM dbmail_mailboxes "
                . "INNER JOIN dbmail_users ON dbmail_mailboxes.owner_idnr = dbmail_users.user_idnr "
                . "WHERE dbmail_mailboxes.name = '{$this->dbmail->escape($folderRealName)}' "
                . "AND dbmail_users.userid = '{$this->dbmail->escape($folderOwner)}' ";

        $sharedMailboxResult = $this->dbmail->query($sharedMailboxSQL);
        $sharedMailbox = $this->dbmail->fetch_assoc($sharedMailboxResult);

        if (is_array($sharedMailbox) && array_key_exists('mailbox_idnr', $sharedMailbox) && strlen($sharedMailbox['mailbox_idnr'])) {
            /*
             * Shared mailbox found!
             * - return mailbox_idnr
             */

            $this->set_temp_value($temp_key, $sharedMailbox['mailbox_idnr']);

            return $sharedMailbox['mailbox_idnr'];
        } else {
            return FALSE;
        }
    }

    /**
     * Retrieve physmessage id
     *
     * @param int  $message_idnr    message id
     *
     * @return int physmessage on success, False on failure
     */
    protected function get_physmessage_id($message_idnr) {

        $query = " SELECT physmessage_id "
                . " FROM dbmail_messages "
                . " WHERE message_idnr = '{$this->dbmail->escape($message_idnr)}' ";

        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        if (!is_array($row) || !array_key_exists('physmessage_id', $row)) {
            return FALSE;
        }

        return $row['physmessage_id'];
    }

    /**
     * Retrieve user identifier
     *
     * @param string  $user    user name
     *
     * @return int user_idnr on success, False on failure
     */
    protected function get_user_id($user) {

        $query = " SELECT user_idnr "
                . " FROM dbmail_users "
                . " WHERE userid = '{$this->dbmail->escape($user)}'";

        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        return (is_array($row) && array_key_exists('user_idnr', $row) ? $row['user_idnr'] : FALSE);
    }

    /**
     * Retrieve message record
     * @param int $message_idnr
     * @return array
     */
    private function get_message_record($message_idnr) {

        $query = " SELECT * "
                . " FROM dbmail_messages "
                . " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id  "
                . " WHERE message_idnr = '{$this->dbmail->escape($message_idnr)}' ";

        $res = $this->dbmail->query($query);
        if ($this->dbmail->num_rows($res) == 0) {
            // not found
            return FALSE;
        }

        return $this->dbmail->fetch_assoc($res);
    }

    /**
     * Retrieve physmessage record
     * @param int $physmessage_id
     * @return array
     */
    private function get_physmessage_record($physmessage_id) {

        $query = " SELECT * "
                . " FROM dbmail_physmessage "
                . " WHERE id = '{$this->dbmail->escape($physmessage_id)}' ";

        $res = $this->dbmail->query($query);
        if ($this->dbmail->num_rows($res) == 0) {
            // not found
            return FALSE;
        }

        return $this->dbmail->fetch_assoc($res);
    }

    /**
     * Retrieve folder record
     * @param int mailbox_idnr
     * @return array
     */
    private function get_folder_record($mailbox_idnr) {

        $query = " SELECT * "
                . " FROM dbmail_mailboxes "
                . " WHERE mailbox_idnr = '{$this->dbmail->escape($mailbox_idnr)}' ";

        $res = $this->dbmail->query($query);
        if ($this->dbmail->num_rows($res) == 0) {
            // not found
            return FALSE;
        }

        return $this->dbmail->fetch_assoc($res);
    }

    protected function create_message_unique_id($message_idnr = '') {

        return md5(uniqid($message_idnr, true));
    }

    /**
     * Retrieve mailbox sub folders
     *
     * @param string  $folder    folder name
     *
     * @return array sub folders name list on success, False on failure
     */
    protected function get_sub_folders($folder) {

        $sub_folders = array();

        // set target (folder name with trailing delimiter)
        $target = $folder . $this->delimiter;

        $query = " SELECT mailbox_idnr, name "
                . " FROM dbmail_mailboxes "
                . " WHERE name like '{$this->dbmail->escape($target)}%' "
                . " AND owner_idnr = {$this->dbmail->escape($this->user_idnr)} "
                . " AND deleted_flag = 0 ";

        $res = $this->dbmail->query($query);

        while ($row = $this->dbmail->fetch_assoc($res)) {

            $sub_folders[$row['mailbox_idnr']] = $row['name'];
        }

        return $sub_folders;
    }

    /**
     * Retrieve message headers from 'dbmail_mimeparts'
     * @param int $physmessage_id
     * @return array 
     */
    protected function get_physmessage_headers($physmessage_id) {

        $query = "SELECT dbmail_mimeparts.data "
                . " FROM dbmail_partlists "
                . " INNER JOIN dbmail_mimeparts ON dbmail_partlists.part_id = dbmail_mimeparts.id "
                . " WHERE dbmail_partlists.physmessage_id = {$this->dbmail->escape($physmessage_id)} "
                . " AND dbmail_partlists.is_header = 1 "
                . " AND dbmail_partlists.part_depth = 0 ";

        $res = $this->dbmail->query($query);

        if ($this->dbmail->num_rows($res) == 0) {
            return array();
        }

        $row = $this->dbmail->fetch_assoc($res);

        $mime_decode = new Mail_mimeDecode(trim($row['data']));

        $decode_params = array(
            'include_bodies' => TRUE,
            'decode_bodies' => TRUE,
            'decode_headers' => TRUE,
            'rfc_822bodies' => TRUE
        );

        /*
         * add error suppression to avoid "Deprecated:  preg_replace(): The /e modifier 
         * is deprecated, use preg_replace_callback instead in 
         * ..../roundcube/vendor/pear-pear.php.net/Mail_mimeDecode/Mail/mimeDecode.php on line 762"
         */
        $mime_decoded = @$mime_decode->decode($decode_params);

        // add mime_id attributes to '$mime_decoded' array items (pass by reference)
        $mime_decode->getMimeNumbers($mime_decoded);

        return $mime_decoded->headers;
    }

    /**
     * Retrieve delimiter for supplied header name
     */
    protected function get_header_delimiter($token) {

        $match = array('boundary', 'filename', 'x-unix-mode', 'name', 'charset', 'format', 'size');

        $delimiter = ": ";
        foreach ($match as $item) {
            if (strtoupper(substr($token, 0, strlen($item))) == strtoupper($item)) {
                $delimiter = "=";
                break;
            }
        }

        return $delimiter;
    }

    /**
     * Search for a specific header name within supplied headers list (case insensitive match!!!!!)
     * 
     * @param string $header headers list
     * @param string $token header name 
     * 
     * @return string header value on success, False when not found
     */
    protected function get_header_value($header, $token) {

        /*
         * Init header value container
         */
        $header_value = FALSE;

        /*
         * Init language / content encoding containers
         */
        $content_encoding = FALSE;
        $content_language = FALSE;

        /*
         * Remove any trailing WSP
         */
        $header = trim($header);

        /*
         * Unfolding according to RFC 2822, chapter 2.2.3
         */
        $header = str_replace("\r\n ", " ", $header);
        $header = str_replace("\r\n\t", " ", $header);

        /*
         * Unfolding with compatibility with some non-standard mailers
         * that only add \n instead of \r\n
         */
        $header = str_replace("\n ", " ", $header);
        $header = str_replace("\n\t", " ", $header);

        /*
         *  explode headers on new line
         */
        $rows = explode("\n", $header);

        /*
         *  standard delimiter is ':', match '=' only for following properties
         */
        $delimiter = $this->get_header_delimiter($token);

        /*
         *  convert token to uppercase to perform case-insensitive search
         */
        $ci_token = strtoupper($token);

        /*
         * loop each row searching for supplied token
         */
        foreach ($rows as &$row) {

            /*
             *  trim whitespaces
             */
            $row = trim($row);

            /*
             *  split row by ';' to manage multiple key=>value pairs within same row
             */
            $items = explode(';', $row);

            foreach ($items as &$item) {

                $item = trim($item);

                /*
                 * Parameter Value Continuations
                 * 
                 * Long MIME media type or disposition parameter values do not interact
                 * well with header line wrapping conventions.  In particular, proper
                 * header line wrapping depends on there being places where linear
                 * whitespace (LWSP) is allowed, which may or may not be present in a
                 * parameter value, and even if present may not be recognizable as such
                 * since specific knowledge of parameter value syntax may not be
                 * available to the agent doing the line wrapping. The result is that
                 * long parameter values may end up getting truncated or otherwise
                 * damaged by incorrect line wrapping implementations.
                 * 
                 * A mechanism is therefore needed to break up parameter values into
                 * smaller units that are amenable to line wrapping. Any such mechanism
                 * MUST be compatible with existing MIME processors. This means that
                 * 
                 * (1)   the mechanism MUST NOT change the syntax of MIME media
                 *       type and disposition lines, and
                 *       
                 * (2)   the mechanism MUST NOT depend on parameter ordering
                 *       since MIME states that parameters are not order
                 *       sensitive.  Note that while MIME does prohibit
                 *       modification of MIME headers during transport, it is
                 *       still possible that parameters will be reordered when
                 *       user agent level processing is done.
                 *       
                 * The obvious solution, then, is to use multiple parameters to contain
                 * a single parameter value and to use some kind of distinguished name
                 * to indicate when this is being done.  And this obvious solution is
                 * exactly what is specified here: The asterisk character ("*") followed
                 * by a decimal count is employed to indicate that multiple parameters
                 * are being used to encapsulate a single parameter value.  The count
                 * starts at 0 and increments by 1 for each subsequent section of the
                 * parameter value.  Decimal values are used and neither leading zeroes
                 * nor gaps in the sequence are allowed.
                 * 
                 * The original parameter value is recovered by concatenating the
                 * various sections of the parameter, in order.  For example, the
                 * content-type field
                 * 
                 *   Content-Type: message/external-body; access-type=URL;
                 *   URL*0="ftp://";
                 *   URL*1="cs.utk.edu/pub/moore/bulk-mailer/bulk-mailer.tar"
                 *   
                 * is semantically identical to
                 * 
                 *   Content-Type: message/external-body; access-type=URL;
                 *   URL="ftp://cs.utk.edu/pub/moore/bulk-mailer/bulk-mailer.tar"
                 *   
                 * Note that quotes around parameter values are part of the value
                 * syntax; they are NOT part of the value itself.  Furthermore, it is
                 * explicitly permitted to have a mixture of quoted and unquoted
                 * continuation fields.
                 * 
                 * 
                 * 
                 * =================================================================
                 * 
                 * 
                 * Combining Character Set, Language, and Parameter Continuations
                 *   
                 * Character set and language information may be combined with the
                 * parameter continuation mechanism. For example:
                 *   
                 * Content-Type: application/x-stuff
                 * title*0*=us-ascii'en'This%20is%20even%20more%20
                 * title*1*=%2A%2A%2Afun%2A%2A%2A%20
                 * title*2="isn't it!"
                 *   
                 * Note that:
                 *   
                 * (1)   Language and character set information only appear at
                 *       the beginning of a given parameter value.
                 *       
                 * (2)   Continuations do not provide a facility for using more
                 *       than one character set or language in the same
                 *       parameter value.
                 *       
                 * (3)   A value presented using multiple continuations may
                 *       contain a mixture of encoded and unencoded segments.
                 *       
                 * (4)   The first segment of a continuation MUST be encoded if
                 *       language and character set information are given.
                 *       
                 * (5)   If the first segment of a continued parameter value is
                 *       encoded the language and character set field delimiters
                 *       MUST be present even when the fields are left blank.
                 * 
                 * https://tools.ietf.org/html/rfc2231
                 */

                if (preg_match("/^{$ci_token}\*0\*{$delimiter}/i", $item)) {
                    /*
                     * - multi-line property
                     * - first occurrence
                     * - language and encoding supplied (they could be empty though)
                     * - must decode value
                     * 
                     * example: title*0*=us-ascii'en'This%20is%20even%20more%20
                     */
                    list($key, $value) = explode($delimiter, $item, 2);

                    /*
                     * Split content on:
                     * 1. content encoding
                     * 2. language
                     * 3. value
                     */
                    $exploded = explode("'", $value);

                    $content_encoding = strtoupper($exploded[0]);
                    $content_language = strtoupper($exploded[1]);

                    $value = implode("'", array_slice($exploded, 2));

                    /*
                     * Content encoding supplied?
                     */
                    if (strlen($content_encoding) > 0 && $content_encoding != 'UTF-8') {
                        $value = mb_convert_encoding($value, 'UTF-8', $content_encoding);
                    }

                    /*
                     * Decode ASCII chars (if present)
                     */
                    $value = urldecode($value);
                } elseif (preg_match("/^{$ci_token}\*\d\*{$delimiter}/i", $item)) {

                    /*
                     * - multi-line property
                     * - following occurrence
                     * - language and encoding (if any) are supplied within first occurrence
                     * - must decode value
                     * 
                     * example: title*1*=%2A%2A%2Afun%2A%2A%2A%20
                     */
                    list($key, $value) = explode($delimiter, $item, 2);

                    /*
                     * Content encoding supplied?
                     */
                    if (strlen($content_encoding) > 0 && $content_encoding != 'UTF-8') {
                        $value = mb_convert_encoding($value, 'UTF-8', $content_encoding);
                    }

                    /*
                     * Decode ASCII chars (if present)
                     */
                    $value = urldecode($value);
                } elseif (preg_match("/^{$ci_token}\*\d{$delimiter}/i", $item)) {

                    /*
                     * - multi-line property
                     * - must NOT decode value
                     * 
                     * example: title*2="isn't it!"
                     */
                    list($key, $value) = explode($delimiter, $item, 2);
                } elseif (preg_match("/^{$ci_token}{$delimiter}/i", $item)) {
                    /*
                     * - single-line property
                     * - must NOT decode value
                     * 
                     * example: title=myTitle
                     */
                    list($key, $value) = explode($delimiter, $item, 2);
                } else {
                    /*
                     * Doesn't match!
                     */
                    continue;
                }

                /*
                 * remove trailing / leading:
                 * - spaces 
                 * - quotes
                 * - double quotes
                 * from $value
                 */
                $value = trim($value);

                if (strlen($value) > 2 && substr($value, 0, 1) == "'" && substr($value, -1) == "'") {
                    $value = trim($value, "'");
                } elseif (strlen($value) > 2 && substr($value, 0, 1) == "\"" && substr($value, -1) == "\"") {
                    $value = trim($value, "\"");
                }

                /*
                 * Init header value to empty string (if needed) 
                 */
                if (strlen($header_value) == 0) {
                    $header_value = '';
                }

                $header_value .= $value;
            }
        }

        return $header_value;
    }

    /**
     * Return header name id
     *
     * @param string  $header_name    search criteria
     *
     * @return int headerID on success, False on failure
     */
    private function get_header_id_by_header_name($header_name) {

        /**
          Roundcube doesn't use some standard RFC names, so we have
          to normalize this.
          Roundcube "arrival" corresponds to "received", but - WARNING -
          received isn't a required header, so the results are pretty strange
          maybe assuming arrival == data could be a better solution
         */
        if ($header_name == "arrival") {
            $header_name = "received";
        }

        /**
         * Search for cached items
         */
        $cache_key = "HEADERS_LOOKUP";
        $headers = $this->get_cache($cache_key);
        if (is_array($headers) && array_key_exists($header_name, $headers)) {
            return $headers[$header_name];
        }

        /**
         * Not found - cache 'dbmail_headername' lookup table
         */
        $sql = "SELECT * "
                . "FROM dbmail_headername ";

        $res = $this->dbmail->query($sql);

        $headers = array();
        while ($row = $this->dbmail->fetch_assoc($res)) {
            $headers[$row['headername']] = $row['id'];
        }

        /**
         * Cache headers lookup
         */
        $this->update_cache($cache_key, $headers);

        /**
         * return result
         */
        return (is_array($headers) && array_key_exists($header_name, $headers) ? $headers[$header_name] : FALSE);
    }

    /**
     * Return header value id
     *
     * @param string  $header_value    search criteria
     *
     * @return int headerNameID on success, False on failure
     */
    private function get_header_value_id_by_header_value($header_value) {

        $query = "SELECT id "
                . "FROM dbmail_headervalue "
                . "WHERE headervalue = '{$this->dbmail->escape($header_value)}' "
                . "LIMIT 1";

        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);
        return (is_array($row) && array_key_exists('id', $row) ? $row['id'] : FALSE);
    }

    /**
     * Keyword exists?  
     *
     * @param int  $message_idnr    message ID
     * @param string  $keyword    keyword
     *
     * @return boolean TRUE if keyword exists, FALSE otherwise
     */
    private function get_keyword($message_idnr, $keyword) {

        $query = "SELECT * "
                . "FROM dbmail_keywords "
                . "WHERE message_idnr = '{$this->dbmail->escape($message_idnr)}'"
                . "AND keyword = '{$this->dbmail->escape($keyword)}'";

        $res = $this->dbmail->query($query);

        return ($this->dbmail->num_rows($res) == 0 ? FALSE : TRUE);
    }

    /**
     * Set Keyword
     *
     * @param int  $message_idnr    message ID
     * @param string  $keyword    keyword
     * @return boolean TRUE on success, FALSE otherwise
     */
    private function set_keyword($message_idnr, $keyword) {

        // keyword already exists?
        if ($this->get_keyword($message_idnr, $keyword)) {
            // OK - found!
            return TRUE;
        }

        // insert keyword
        $query = "INSERT INTO dbmail_keywords "
                . "(message_idnr, keyword) "
                . "VALUES "
                . "('{$this->dbmail->escape($message_idnr)}', '{$this->dbmail->escape($keyword)}') ";

        return ($this->dbmail->query($query) ? TRUE : FALSE);
    }

    /**
     * Function to retrieve a message.
     * In it's basic for this function, given a message_idnr, retrieves the full data set
     * and cache the resultset into roundcube fast-lookup cache.
     *
     * Furthermore it can also receive the "message_data" array, thus avoiding the database lookup
     * (and the potentially cached result) for the message flags and the physmessage_id
     *
     * It can optionally receive a specific physical message if in the message_data array in order to skip
     * the physical message id lookup, if the caller aready has this information (for example
     * when called from _list_messages).
     * 
     * @param int $message_idnr
     * @param string $folder
     * @param array $message_data 
     * @param boolean $getBody whenever to retrieve body content too (instead of headers only)
     * @return mixed
     */
    private function retrieve_message($message_idnr, $folder, $message_data = FALSE, $getBody = TRUE) {

        /*
         * Get cached contents
         */
        $rcmh_cached_key = "MSG_" . $message_idnr;
        $rcmh_cached = $this->get_cache($rcmh_cached_key);

        /*
         * Checklist:
         *  - Is the object in cache a valid object?
         *  - Do we need (and do we have) the message body?
         */
        if (is_object($rcmh_cached) &&
                (!$getBody || isset($rcmh_cached->structure))) {

            /*
             * If we're in the message list we certainly have an up-to-date message listing
             */
            if (is_array($message_data) && !empty($message_data)) {

                $rcmh_cached->folder = $message_data['folder_record']['name'];
                $rcmh_cached->flags["SEEN"] = ($message_data['seen_flag'] == 1 ? TRUE : FALSE);
                $rcmh_cached->flags["ANSWERED"] = ($message_data['answered_flag'] == 1 ? TRUE : FALSE);
                $rcmh_cached->flags["DELETED"] = ($message_data['deleted_flag'] == 1 ? TRUE : FALSE);
                $rcmh_cached->flags["FLAGGED"] = ($message_data['flagged_flag'] == 1 ? TRUE : FALSE);
                $rcmh_cached->flags["FORWARDED"] = $this->get_keyword($message_idnr, self::KEYWORD_FORWARDED);
                $rcmh_cached->flags["SKIP_MBOX_CHECK"] = TRUE;
            }

            /**
             * Threaded view enabled? Make sure to return thread details too!
             */
            if ($this->get_threading() &&
                    isset($message_data["physmessage_id"]) &&
                    (!isset($rcmh_cached->base_thread_message_id) || strlen($rcmh_cached->base_thread_message_id) == 0 || !isset($rcmh_cached->ancesters) )) {

                /*
                 * retrieve thread details 
                 */
                $thread_details = $this->_get_thread_details($message_data["physmessage_id"], $rcmh_cached->id, $rcmh_cached->folder);

                $rcmh_cached->base_thread_message_id = $thread_details->base_thread_message_id;
                $rcmh_cached->ancesters = $thread_details->ancesters;

                /*
                 * update cached contents
                 */
                $this->update_cache($rcmh_cached_key, $rcmh_cached);
            }

            return $rcmh_cached;
        }

        /*
         * Get message data (if not supplied)
         */
        if (!$message_data) {

            /*
             * Get message properties
             */
            $message_record = $this->get_message_record($message_idnr);
            if (!$message_record) {
                // Not found!
                return FALSE;
            }

            /*
             * Ok - prepare message data array
             */
            $message_data = array(
                'message_idnr' => $message_record["message_idnr"],
                'unique_id' => $message_record["unique_id"],
                'physmessage_id' => $message_record['physmessage_id'],
                'message_size' => $message_record["messagesize"],
                'seen_flag' => $message_record["seen_flag"],
                'answered_flag' => $message_record["answered_flag"],
                'deleted_flag' => $message_record["deleted_flag"],
                'flagged_flag' => $message_record["flagged_flag"],
                'folder_record' => array(
                    'name' => $folder
                ),
                'mailbox_idnr' => $message_record['mailbox_idnr']
            );
        }

        // extract mime parts
        $mime = $this->fetch_part_lists($message_data['physmessage_id']);

        // prepare response
        $rcmh = new rcube_message_header();
        $rcmh->id = $message_idnr;
        $rcmh->uid = $message_idnr;
        $rcmh->folder = $message_data['folder_record']['name'];
        $rcmh->subject = $this->get_header_value($mime->header, 'Subject');
        $rcmh->from = $this->get_header_value($mime->header, 'From');
        $rcmh->to = $this->get_header_value($mime->header, 'To');
        $rcmh->cc = $this->get_header_value($mime->header, 'Cc');
        $rcmh->bcc = $this->get_header_value($mime->header, 'Bcc');
        $rcmh->replyto = $this->get_header_value($mime->header, 'Reply-To');
        $rcmh->in_reply_to = $this->get_header_value($mime->header, 'In-Reply-To');
        $rcmh->ctype = $this->get_header_value($mime->header, 'Content-Type');
        $rcmh->references = $this->get_header_value($mime->header, 'References');
        $rcmh->mdn_to = $this->get_header_value($mime->header, 'Return-Receipt-To');
        $rcmh->priority = $this->get_header_value($mime->header, 'X-Priority');
        $rcmh->date = $this->get_header_value($mime->header, 'Date');
        $rcmh->internaldate = $this->get_header_value($mime->header, 'Date');
        $rcmh->messageID = $this->get_header_value($mime->header, 'Message-ID');
        $rcmh->size = $message_data["message_size"];
        $rcmh->timestamp = time();
        $rcmh->flags["SEEN"] = ($message_data['seen_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["ANSWERED"] = ($message_data['answered_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["DELETED"] = ($message_data['deleted_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["FLAGGED"] = ($message_data['flagged_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["SKIP_MBOX_CHECK"] = TRUE;

        if ($getBody) {

            $mime_decoded = $this->decode_raw_message($mime->header . $mime->body);
            if (!$mime_decoded) {
                return FALSE;
            }

            $rcmh->structure = $this->get_structure($mime_decoded);
        }

        /**
         * Threaded view enabled? Make sure to return thread details too!
         */
        if ($this->get_threading() &&
                isset($message_data["physmessage_id"]) &&
                (!isset($rcmh_cached->base_thread_message_id) || strlen($rcmh_cached->base_thread_message_id) == 0 || !isset($rcmh_cached->ancesters) )) {

            /*
             * retrieve thread details 
             */
            $thread_details = $this->_get_thread_details($message_data['physmessage_id'], $rcmh_cached->id, $rcmh_cached->folder);

            $rcmh->base_thread_message_id = $thread_details->base_thread_message_id;
            $rcmh->ancesters = $thread_details->ancesters;
        }

        // update cached contents
        $this->update_cache($rcmh_cached_key, $rcmh);

        return $rcmh;
    }

    /**
     * Retrieve mailbox 'seq' flag 
     * @param int $mailbox_idnr
     * @return int 'seq' flag
     */
    private function get_mailbox_seq($mailbox_idnr) {


        $query = "SELECT seq "
                . " FROM dbmail_mailboxes "
                . " WHERE mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)}";

        $result = $this->dbmail->query($query);

        $row = $this->dbmail->fetch_assoc($result);

        return isset($row['seq']) && strlen($row['seq']) > 0 ? $row['seq'] : FALSE;
    }

    /**
     * Increment 'seq' flag for supplied mailbox ID and if a message_id is specified
     * increment it as well
     *
     * @param int  $mailbox_idnr    mailbox ID
     * @param int  $message_idnr    message ID
     *
     * @return boolean True on success, False on failure
     */
    private function increment_mailbox_seq($mailbox_idnr, $message_idnr = FALSE) {

        if ($message_idnr == FALSE) {
            $query = "UPDATE dbmail_mailboxes "
                    . " SET seq = (seq + 1) "
                    . " WHERE mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)}";
        } else {

            $query = "UPDATE  dbmail_mailboxes, dbmail_messages
                        SET   dbmail_mailboxes.seq = dbmail_mailboxes.seq + 1, dbmail_messages.seq = dbmail_mailboxes.seq
                        WHERE dbmail_mailboxes.mailbox_idnr = " . $mailbox_idnr . "
                        AND   dbmail_messages.message_idnr = " . $message_idnr;
        }

        return ($this->dbmail->query($query) ? TRUE : FALSE);
    }

    /**
     * Checks if folder is subscribed
     *
     * @param int  $mailbox_idnr    mailbox ID
     *
     * @return boolean True if is subscribed, False otherwise
     */
    private function folder_subscription_exists($mailbox_idnr) {

        $query = "SELECT mailbox_id "
                . " FROM dbmail_subscription "
                . " WHERE user_id = {$this->dbmail->escape($this->user_idnr)} "
                . " AND mailbox_id = {$this->dbmail->escape($mailbox_idnr)}";

        $res = $this->dbmail->query($query);

        return ($this->dbmail->num_rows($res) == 1 ? TRUE : FALSE);
    }

    /**
     * Format supplied message UIDs list
     *
     * @param mixed  $uids  Message UIDs as array or comma-separated string, or '*'
     * @param string $folder    Target folder
     *
     * @return array message UIDs list on success, False on failure
     */
    protected function list_message_UIDs($uids, $folder = '', $onlyDeleted = FALSE) {

        $message_UIDs = array();

        if (is_string($uids) && ($uids == '*' || $uids == '1:*') && strlen($folder) > 0) {

            // full folder request
            $mailbox_idnr = $this->get_mail_box_id($folder);

            $query = "SELECT message_idnr "
                    . " FROM dbmail_messages "
                    . " WHERE mailbox_idnr = '{$this->dbmail->escape($mailbox_idnr)}'";

            if ($onlyDeleted == TRUE) {
                $query = $query . " AND deleted_flag = 1";
            }

            $res = $this->dbmail->query($query);

            if ($this->dbmail->num_rows($res) == 0) {
                // nothing found
                return FALSE;
            }

            // fetch result set
            while ($row = $this->dbmail->fetch_assoc($res)) {
                $message_UIDs[] = $row['message_idnr'];
            }
        } elseif (is_array($uids) && !empty($uids)) {

            // array supplied - do nothing
            $message_UIDs = $uids;
        } elseif (strlen($uids) > 0) {

            //comma-separated string or single UID supplied
            $message_UIDs = explode(',', $uids);
        }

        return (count($message_UIDs) > 0 ? $message_UIDs : FALSE);
    }

    /**
     * Parameters are supplied in a mixed way:
     * eg. NOT HEADER X-PRIORITY 1 NOT HEADER X-PRIORITY 2 NOT HEADER X-PRIORITY 4 NOT HEADER X-PRIORITY 5 OR OR OR OR OR HEADER SUBJECT 123456 HEADER FROM 123456 HEADER TO 123456 HEADER CC 123456 HEADER BCC 123456 BODY 123456
     * 
     * where:
     * first block (if present) contains filters ('NOT HEADER X-PRIORITY 1 NOT HEADER X-PRIORITY 2 NOT HEADER X-PRIORITY 4 NOT HEADER X-PRIORITY 5')
     * second block (if present) contains search parameters in polish notation ('OR OR OR OR OR HEADER SUBJECT 123456 HEADER FROM 123456 HEADER TO 123456 HEADER CC 123456 HEADER BCC 123456 BODY 123456')
     * 
     * @param string $str search string
     * @return stdClass
     */
    private function format_search_parameters($str) {

        $filter_str = $_SESSION['search_filter'];
        $search_str = '';
        if (strlen($filter_str) > 0 && substr($str, 0, strlen($filter_str)) == $filter_str) {
            // filter supplied, remove them from '$str'
            $search_str = trim(substr($str, strlen($filter_str)));
        } else {
            $search_str = $str;
        }

        $response = new stdClass();
        $response->filters = $filter_str;
        $response->search = $search_str;

        return $response;


        $formatted_search = array();
        $formatted_filter = array();

        /*
         * init response container
         */
        $response = new stdClass();
        $response->formatted_filter_str = '';
        $response->formatted_search_str = '';
        $response->additional_join_tables = FALSE;
        $search_join_headers_table = FALSE;
        $search_join_partlist_table = FALSE;
        $filter_join_headers_table = FALSE;

        /*
         * Search string management 
         */
        if (strlen($search_str) > 0) {

            $exploded_search_str = explode(" ", $search_str);
            $current_index = 0;

            while ($exploded_search_str) {

                $current_item = (array_key_exists($current_index, $exploded_search_str) ? $exploded_search_str[$current_index] : FALSE);

                // search for operators
                if (strtoupper($current_item) != 'OR') {

                    // search term could contain multiple words, try to match them
                    $search_term_offset = $current_index + (strtoupper($current_item) == 'HEADER' ? 2 : 1);

                    $search_term = trim($exploded_search_str[$search_term_offset]);

                    // remove item from list
                    unset($exploded_search_str[$search_term_offset]);

                    //  search term begins with double quotes?
                    if (substr($search_term, 0, 1) == '"') {

                        // remove leading double quotes (multi words search term)
                        $search_term = ltrim($search_term, '"');

                        $exploded_search_str = array_values($exploded_search_str);
                        $exploded_search_str_length = count($exploded_search_str);

                        for ($search_term_offset = $search_term_offset++; $search_term_offset < $exploded_search_str_length; $search_term_offset++) {

                            $next_item = trim($exploded_search_str[$search_term_offset]);

                            // remove item from list
                            unset($exploded_search_str[$search_term_offset]);

                            // item ends with double quotes?
                            if (substr($next_item, -1) == '"') {
                                // ok - last element
                                $search_term .= " " . substr($next_item, 0, -1);
                                break;
                            } else {
                                $search_term .= " " . $next_item;
                            }
                        }
                    }

                    if (strtoupper($current_item) == 'HEADER') {

                        // 3 items - eg. HEADER FROM 123456
                        $header_name = strtolower($exploded_search_str[($current_index + 1)]);
                        $header_id = $this->get_header_id_by_header_name($header_name);

                        unset($exploded_search_str[$current_index]);
                        unset($exploded_search_str[($current_index + 1)]);

                        $search_join_headers_table = TRUE;

                        $formatted_search[] = " "
                                . " ( "
                                . "     search_dbmail_header.headername_id = '{$this->dbmail->escape($header_id)}' "
                                . "     AND search_dbmail_headervalue.headervalue LIKE '%{$this->dbmail->escape($search_term)}%' "
                                . " ) ";
                    } elseif (strtoupper($current_item) == 'BODY') {
                        // 2 items - eg.  BODY 123456

                        $search_join_partlist_table = TRUE;

                        $formatted_search[] = " "
                                . " ( "
                                . "     search_dbmail_partlists.is_header = 0 "
                                . "     AND search_dbmail_mimeparts.data LIKE '%{$this->dbmail->escape($search_term)}%' "
                                . " ) ";

                        unset($exploded_search_str[$current_index]);
                    } elseif (strtoupper($current_item) == 'TEXT') {
                        // 2 items - eg.  TEXT 123456

                        $search_join_partlist_table = TRUE;

                        $formatted_search[] = " "
                                . " ( "
                                . "     search_dbmail_partlists.is_header = 0 "
                                . "     AND search_dbmail_mimeparts.data LIKE '%{$this->dbmail->escape($search_term)}%' "
                                . " ) ";

                        unset($exploded_search_str[$current_index]);
                    } else {
                        // unsupported operand - break!
                        return FALSE;
                    }

                    if (array_key_exists(($current_index - 1), $exploded_search_str)) {
                        // remove leading operator (if found)
                        unset($exploded_search_str[($current_index - 1)]);
                    }

                    // restart fetch on updated $exploded_search_str array
                    $exploded_search_str = array_values($exploded_search_str);
                    $current_index = 0;
                    continue;
                } else {
                    // operator - skip to next item
                    $current_index++;
                    continue;
                }
            }
        }

        // implode formatted_search array
        if (count($formatted_search) > 0) {
            $response->formatted_search_str = " ( " . implode(' OR ', $formatted_search) . " ) ";
        }

        /*
         * Filters management
         */
        if (strlen($filter_str) > 0) {
            switch ($filter_str) {
                case 'ALL':
                    // DO NOTHING!!!!
                    break;
                case 'UNSEEN':

                    $formatted_filter[] = " dbmail_messages.seen_flag = 0 ";

                    break;
                case 'FLAGGED':

                    $formatted_filter[] = " dbmail_messages.flagged_flag = 1 ";

                    break;
                case 'UNANSWERED':

                    $formatted_filter[] = " dbmail_messages.answered_flag = 0 ";

                    break;
                case 'DELETED':

                    $formatted_filter[] = " dbmail_messages.deleted_flag = 1 ";

                    break;
                case 'UNDELETED':

                    $formatted_filter[] = " dbmail_messages.deleted_flag = 0 ";

                    break;
                case 'OR OR OR HEADER Content-Type application/ HEADER Content-Type multipart/m HEADER Content-Type multipart/signed HEADER Content-Type multipart/report':

                    $filter_join_headers_table = TRUE;

                    $header_id = $this->get_header_id_by_header_name('content-type');

                    $formatted_filter[] = " "
                            . " ( "
                            . "    filter_dbmail_header.headername_id = '{$this->dbmail->escape($header_id)}' "
                            . "    AND "
                            . "        ( "
                            . "             filter_dbmail_headervalue.headervalue LIKE 'application/%' "
                            . "             OR filter_dbmail_headervalue.headervalue LIKE 'multipart/%' "
                            . "        ) "
                            . " ) ";

                    break;
                case 'HEADER X-PRIORITY 1':

                    $filter_join_headers_table = TRUE;

                    $header_id = $this->get_header_id_by_header_name('x-priority');

                    $formatted_filter[] = " "
                            . " ( "
                            . "    filter_dbmail_header.headername_id = '{$this->dbmail->escape($header_id)}' "
                            . "    AND filter_dbmail_headervalue.headervalue = 1 "
                            . " ) ";

                    break;
                case 'HEADER X-PRIORITY 2':

                    $filter_join_headers_table = TRUE;

                    $header_id = $this->get_header_id_by_header_name('x-priority');

                    $formatted_filter[] = " "
                            . " ( "
                            . "    filter_dbmail_header.headername_id = '{$this->dbmail->escape($header_id)}' "
                            . "    AND filter_dbmail_headervalue.headervalue = 2 "
                            . " ) ";

                    break;
                case 'NOT HEADER X-PRIORITY 1 NOT HEADER X-PRIORITY 2 NOT HEADER X-PRIORITY 4 NOT HEADER X-PRIORITY 5':

                    // normal priority

                    $filter_join_headers_table = TRUE;

                    $header_id = $this->get_header_id_by_header_name('x-priority');

                    $formatted_filter[] = " "
                            . " ( "
                            . "    filter_dbmail_header.headername_id = '{$this->dbmail->escape($header_id)}' "
                            . "    AND filter_dbmail_headervalue.headervalue = 3 "
                            . " ) ";

                    break;
                case 'HEADER X-PRIORITY 4':

                    $filter_join_headers_table = TRUE;

                    $header_id = $this->get_header_id_by_header_name('x-priority');

                    $formatted_filter[] = " "
                            . " ( "
                            . "    filter_dbmail_header.headername_id = '{$this->dbmail->escape($header_id)}' "
                            . "    AND filter_dbmail_headervalue.headervalue = 4 "
                            . " ) ";

                    break;
                case 'HEADER X-PRIORITY 5':

                    $filter_join_headers_table = TRUE;

                    $header_id = $this->get_header_id_by_header_name('x-priority');

                    $formatted_filter[] = " "
                            . " ( "
                            . "    filter_dbmail_header.headername_id = '{$this->dbmail->escape($header_id)}' "
                            . "    AND filter_dbmail_headervalue.headervalue = 5 "
                            . " ) ";

                    break;
                default:
                    // unsupported filter - EXIT!
                    return FALSE;
            }
        }

        // implode formatted_filter array
        if (count($formatted_filter) > 0) {
            $response->formatted_filter_str = " ( " . implode(' AND ', $formatted_filter) . " ) ";
        }

        // set additional joins needed
        if ($search_join_headers_table) {
            $response->additional_join_tables .= " "
                    . " INNER JOIN dbmail_header AS search_dbmail_header ON dbmail_physmessage.id = search_dbmail_header.physmessage_id "
                    . " INNER JOIN dbmail_headervalue AS search_dbmail_headervalue ON search_dbmail_header.headervalue_id = search_dbmail_headervalue.id ";
        }

        if ($search_join_partlist_table) {
            $response->additional_join_tables .= " "
                    . " INNER JOIN dbmail_partlists AS search_dbmail_partlists ON dbmail_physmessage.id = search_dbmail_partlists.physmessage_id AND search_dbmail_partlists.is_header = 0 "
                    . " INNER JOIN dbmail_mimeparts AS search_dbmail_mimeparts ON search_dbmail_partlists.part_id = search_dbmail_mimeparts.id ";
        }

        if ($filter_join_headers_table) {
            $response->additional_join_tables .= " "
                    . " INNER JOIN dbmail_header AS filter_dbmail_header ON dbmail_physmessage.id = filter_dbmail_header.physmessage_id "
                    . " INNER JOIN dbmail_headervalue AS filter_dbmail_headervalue ON filter_dbmail_header.headervalue_id = filter_dbmail_headervalue.id ";
        }

        return $response;
    }

    /**
     * Private wrapper for messages listing
     *
     * @param   string   $folder     Folder name
     * @param   int      $page       Current page to list
     * @param   string   $sort_field Header field to sort by
     * @param   string   $sort_order Sort order [ASC|DESC]
     * @param   int      $slice      Number of slice items to extract from result array
     * @param   string   $search_str
     *
     * @return  array    Indexed array with message header objects
     */
    private function _list_messages($folders = null, $page = null, $sort_field = null, $sort_order = null, $slice = 0, $search_str = NULL) {

        if (!is_array($folders) || count($folders) == 0) {
            /*
             *  no mailboxes supplied!
             */
            return FALSE;
        }

        /*
         *  map mailboxes ID
         */
        $mailboxes = array();
        foreach ($folders as $folder_name) {

            /*
             *  Retrieve mailbox ID
             */
            $mail_box_idnr = $this->get_mail_box_id($folder_name);
            if (!$mail_box_idnr) {
                // Not found - Skip!
                return FALSE;
            }

            /*
             *  ACLs check ('lookup' and 'read' grants required )
             */
            $ACLs = $this->_get_acl(NULL, $mail_box_idnr);
            if (!is_array($ACLs) || !in_array(self::ACL_LOOKUP_FLAG, $ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
                // Unauthorized - Skip!
                return FALSE;
            }

            /*
             *  Add mailbox ID to mailboxes list
             */
            $mailboxes[$mail_box_idnr] = $folder_name;
        }

        /*
         *  no mailboxes available
         */
        if (count($mailboxes) == 0) {
            return FALSE;
        }

        /*
         * Validate sort order (use default when not supplied)
         */
        if (strtoupper($sort_order) != 'DESC') {
            $sort_order = 'ASC';
        }

        /*
         * Set query offset / limit
         */
        $page = ((int) $page > 0 ? (int) $page : $this->list_page);
        $query_offset = ($page > 0 ? (($page - 1) * $this->page_size) : 0);
        $query_limit = $this->page_size;

        /**
         * Return entries 
         */
        if ($this->get_threading()) {
            return $this->_get_threaded_messages($mailboxes, $search_str, $sort_field, $sort_order, $query_offset, $query_limit);
        } else {
            return $this->_get_listed_messages($mailboxes, $search_str, $sort_field, $sort_order, $query_offset, $query_limit);
        }
    }

    private function _get_list_message_query($mailboxes = array(), $query_offset = null, $query_limit = null, $sort_field = null, $sort_order = null, $search_str = NULL, $target_message_idnrs = array()) {

        /*
         * Init additional join tables list (sort / filters)
         */
        $additional_sort_joins = "";
        $additional_filter_joins = "";

        /*
         * Format search string
         */
        $search_conditions = $this->_translate_search_parameters($search_str);
        if (!$search_conditions) {
            return FALSE;
        }

        /*
         * "Base Condition" is that the message should not be EXPUNGED (thus DELETED) and within target mailboxes
         */
        $where_conditions = " WHERE dbmail_messages.status < " . self::MESSAGE_STATUS_DELETE;
        $where_conditions .= " AND dbmail_messages.mailbox_idnr IN (" . implode(",", array_keys($mailboxes)) . ")";

        /*
         * Apply search criteria
         */
        if (isset($search_conditions->additional_where_conditions) && strlen($search_conditions->additional_where_conditions) > 0) {
            $where_conditions .= " AND {$search_conditions->additional_where_conditions}";
            $additional_filter_joins .= implode(PHP_EOL, $search_conditions->additional_joins);
        }

        /*
         * Do we want deleted messages?
         */
        if ($this->options["skip_deleted"]) {
            $where_conditions .= " AND dbmail_messages.deleted_flag = 0 ";
        }

        /**
         * Is there a specific messages list supplied?
         */
        if (count($target_message_idnrs) > 0) {
            $where_conditions .= " AND dbmail_messages.message_idnr IN (" . implode(",", $target_message_idnrs) . ") ";
        }

        /*
         *  Set 'order by' clause
         */
        switch ($sort_field) {
            case 'subject':
            case 'from':
            case 'to':
            case 'cc':
            case 'arrival':
            case 'date':
                /*
                 *  'subject' / 'from' and 'date' values are stored into 'dbmail_headervalue' table
                 */
                $header_id = $this->get_header_id_by_header_name($sort_field);

                /*
                 * target header could NOT be present, so we use left joins on headers tables
                 */
                $additional_sort_joins = " INNER JOIN dbmail_physmessage AS sort_dbmail_physmessage ON dbmail_messages.physmessage_id = sort_dbmail_physmessage.id "
                        . " LEFT JOIN dbmail_header AS sort_dbmail_header ON sort_dbmail_physmessage.id = sort_dbmail_header.physmessage_id AND sort_dbmail_header.headername_id = {$this->dbmail->escape($header_id)} "
                        . " LEFT JOIN dbmail_headervalue AS sort_dbmail_headervalue ON sort_dbmail_header.headervalue_id = sort_dbmail_headervalue.id ";

                $sort_condition = " ORDER BY sort_dbmail_headervalue.sortfield {$this->dbmail->escape($sort_order)} ";
                break;
            case 'size':
                /*
                 *  'size' value is stored into 'dbmail_physmessage' table
                 */
                $additional_sort_joins = " INNER JOIN dbmail_physmessage AS sort_dbmail_physmessage ON dbmail_messages.physmessage_id = sort_dbmail_physmessage.id ";

                $sort_condition = " ORDER BY sort_dbmail_physmessage.messagesize {$this->dbmail->escape($sort_order)} ";
                break;
            case 'message_idnr';
            default:
                /*
                 * order by primary key (default)
                 */
                $sort_condition = " ORDER BY dbmail_messages.message_idnr {$this->dbmail->escape($sort_order)} ";
                break;
        }

        /*
         * Set 'limit' clause 
         */
        $limit_condition = " LIMIT {$this->dbmail->escape($query_offset)}, {$this->dbmail->escape($query_limit)} ";

        /*
         * When no additional joins needed, avoid 'DISTINCT' clause to speed up query execution
         */
        $distinct_clause = (strlen($additional_filter_joins) > 0 ? 'DISTINCT' : '');

        /*
         *  Prepare base query
         */
        $query = " SELECT $distinct_clause dbmail_messages.message_idnr, dbmail_messages.unique_id, "
                . " dbmail_messages.physmessage_id, dbmail_messages.seen_flag, dbmail_messages.answered_flag, "
                . " dbmail_messages.deleted_flag, dbmail_messages.flagged_flag, dbmail_messages.mailbox_idnr "
                . " FROM dbmail_messages ";

        /*
         * Apply sort joins when needed
         */
        if (strlen($additional_sort_joins) > 0) {
            $query .= " {$additional_sort_joins} ";
        }

        /*
         * Join to dbmail_physmessage when needed 
         */
        if (isset($search_conditions->needs_physmessages) && $search_conditions->needs_physmessages) {
            $query .= " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id ";
        }

        $query .= " {$additional_filter_joins} ";
        $query .= " {$where_conditions} ";
        $query .= " {$sort_condition} ";
        $query .= " {$limit_condition} ";

        return $query;
    }

    /**
     * Retrieve listed messages view entries
     * @param array $mailboxes
     * @param string $search_str
     * @param string $sort_field
     * @param string $sort_order
     * @param int $query_offset
     * @param int $query_limit
     * @return array
     */
    private function _get_listed_messages($mailboxes = array(), $search_str = '', $sort_field = '', $sort_order = 'ASC', $query_offset = 0, $query_limit = 50) {

        /*
         * Build query
         */
        $base_messages_query = $this->_get_list_message_query($mailboxes, $query_offset, $query_limit, $sort_field, $sort_order, $search_str, array());

        /*
         * Execute query
         */
        $base_messages_result = $this->dbmail->query($base_messages_query);

        $headers = array();
        $msg_index = ($query_offset + 1);
        while ($msg = $this->dbmail->fetch_assoc($base_messages_result)) {

            /*
             * Get physmessage properties
             */
            $physmessage_record = $this->get_physmessage_record($msg["physmessage_id"]);
            if (!$physmessage_record) {
                // Not found!
                return FALSE;
            }

            /*
             * Ok - prepare message data array
             */
            $message_data = array(
                'message_idnr' => $msg["message_idnr"],
                'unique_id' => $msg["unique_id"],
                'physmessage_id' => $msg['physmessage_id'],
                'message_size' => $physmessage_record["messagesize"],
                'seen_flag' => $msg["seen_flag"],
                'answered_flag' => $msg["answered_flag"],
                'deleted_flag' => $msg["deleted_flag"],
                'flagged_flag' => $msg["flagged_flag"],
                'folder_record' => array(
                    'name' => $mailboxes[$msg["mailbox_idnr"]]
                ),
                'mailbox_idnr' => $msg['mailbox_idnr']
            );

            /*
             * Retrieve message headers / body
             */
            $headers[$msg_index] = $this->retrieve_message($msg["message_idnr"], $mailboxes[$msg["mailbox_idnr"]], $message_data, FALSE);

            $msg_index++;
        }

        return array_values($headers);
    }

    /**
     * Retrieve threaded messages view entries. 
     * 
     * NOTE!!!!!! 
     * This method is pretty heavier than the default 'list' view!!!!!
     * 
     * @param array $mailboxes
     * @param string $search_str
     * @param string $sort_field
     * @param string $sort_order
     * @param int $query_offset
     * @param int $query_limit
     * @return array
     */
    private function _get_threaded_messages($mailboxes = array(), $search_str = '', $sort_field = '', $sort_order = 'ASC', $query_offset = 0, $query_limit = 50) {

        /*
         * Init base massages container
         */
        $headers = array();
        $msg_index = 0;

        /*
         * Despite of messages list, here we need to retrieve items starting from 
         * offset 0, than slice result according to unique threads found because 
         * extracting 10 records doesn't mean return 10 threads.
         */
        $threads_offset = $query_offset;
        $query_offset = 0;

        /*
         * When building a threaded messages list, we should count unique thread-ID 
         * when fetching messages (50 messages doesn't means 50 threads).
         */
        $fetch_more_records = TRUE;
        $threads_count = 0;
        $threads_id = array();
        $thread_entries_list = array();
        $thread_messages_idnr = array();
        $message_id_header_lookup = array();

        while ($fetch_more_records) {

            /*
             * Build query
             */
            $base_messages_query = $this->_get_list_message_query($mailboxes, $query_offset, $query_limit, $sort_field, $sort_order, $search_str, array());

            /*
             * Execute query
             */
            $base_messages_result = $this->dbmail->query($base_messages_query);

            /*
             * Exit when no more records available 
             */
            if ($this->dbmail->affected_rows($base_messages_result) === 0) {
                $fetch_more_records = FALSE;
                continue;
            }

            /*
             * Fetch result
             */
            while ($msg = $this->dbmail->fetch_assoc($base_messages_result)) {

                /*
                 * Get physmessage properties
                 */
                $physmessage_record = $this->get_physmessage_record($msg["physmessage_id"]);
                if (!$physmessage_record) {
                    continue;
                }

                /*
                 * Ok - prepare message data array
                 */
                $message_data = array(
                    'message_idnr' => $msg["message_idnr"],
                    'unique_id' => $msg["unique_id"],
                    'physmessage_id' => $msg['physmessage_id'],
                    'message_size' => $physmessage_record["messagesize"],
                    'seen_flag' => $msg["seen_flag"],
                    'answered_flag' => $msg["answered_flag"],
                    'deleted_flag' => $msg["deleted_flag"],
                    'flagged_flag' => $msg["flagged_flag"],
                    'folder_record' => array(
                        'name' => $mailboxes[$msg["mailbox_idnr"]]
                    ),
                    'mailbox_idnr' => $msg['mailbox_idnr']
                );

                /*
                 * Retrieve message headers / body
                 */
                $message = $this->retrieve_message($msg["message_idnr"], $mailboxes[$msg["mailbox_idnr"]], $message_data, FALSE);

                /*
                 * Increment threads counters on unique conversations
                 */
                if (!in_array($message->base_thread_message_id, $threads_id) && $threads_count <= ($threads_offset + $query_limit)) {
                    /**
                     * Unique thread found. 
                     * NOTE!!!!!! Is really important that unique thread identifiers 
                     * get's appended to thread list according to extraction order, 
                     * so we can slice them later on according to user pagination!
                     */
                    $threads_id[] = $message->base_thread_message_id;
                    $threads_count++;
                } elseif (!in_array($message->base_thread_message_id, $threads_id)) {
                    /**
                     * Unique threads count limit reached
                     */
                    $fetch_more_records = FALSE;
                    break;
                }

                /*
                 * Init base thread properties
                 */
                $message->parent_uid = NULL;
                $message->depth = 0;
                $message->unread_children = 0;
                $message->has_children = FALSE;

                /*
                 * Format 'message-id' header according to 'dbmail_referencesfield' table content
                 */
                $message->thread_message_id = $this->clean_up_thread_message_id($message->messageID);

                /*
                 * Populate message_idnrs list
                 */
                $thread_messages_idnr[$msg["message_idnr"]] = $msg['physmessage_id'];

                /*
                 * Prevent duplicated items
                 */
                if (!array_key_exists($message->thread_message_id, $message_id_header_lookup)) {

                    /*
                     * Add message to base container
                     */
                    $msg_index++;
                    $headers[$msg_index] = $message;

                    /*
                     * Set message-id header lookup
                     */
                    $message_id_header_lookup[$message->thread_message_id] = new stdClass();
                    $message_id_header_lookup[$message->thread_message_id]->msg_index = $msg_index;
                    $message_id_header_lookup[$message->thread_message_id]->message_idnr = $msg["message_idnr"];
                    $message_id_header_lookup[$message->thread_message_id]->physmessage_id = $msg['physmessage_id'];
                    $message_id_header_lookup[$message->thread_message_id]->folder = $mailboxes[$msg["mailbox_idnr"]];
                }
            }

            /*
             * Update offset before executing another query
             */
            $query_offset += $query_limit;
        }

        /*
         * At this point, we slice unique threads list according to pagination.
         */
        $threads_id = array_slice($threads_id, $threads_offset);
        
        /*
         * Now we can remove from '$headers' list messages related to unnecessary threads
         */
        foreach ($headers as $msg_index => $message) {
            if (!in_array($message->base_thread_message_id, $threads_id)) {
                unset($message_id_header_lookup[$message->thread_message_id]);
                unset($headers[$msg_index]);
            }
        }

        /*
         * Foreach unique thread found, search for related messages within dbmail 'referencesfield' table.
         * We will use this list to compare extracted messages and retrieve missing ones.
         */
        foreach ($threads_id as $thread_id) {
            foreach ($this->get_thread_related_message_idnrs($thread_id) as $message_idnr => $physmessage_id) {
                if (!array_key_exists($message_idnr, $thread_messages_idnr)) {
                    $thread_entries_list[] = $message_idnr;
                }
            }
        }

        /*
         * Now we can retrieve missing threaded messages details. 
         * Here we fetch all missing referenced messages.
         */
        if (count($thread_entries_list) > 0) {

            /**
             * Build query to retrieve missing messages. 
             * Here we don't need a specific sort order so we order by primary key to speed up query execution.
             */
            $missing_threaded_messages_query = $this->_get_list_message_query($mailboxes, 0, count($thread_entries_list), 'message_idnr', 'asc', $search_str, $thread_entries_list);

            /*
             * Execute query
             */
            $missing_threaded_messages_result = $this->dbmail->query($missing_threaded_messages_query);

            while ($msg = $this->dbmail->fetch_assoc($missing_threaded_messages_result)) {

                /*
                 * Get physmessage properties
                 */
                $physmessage_record = $this->get_physmessage_record($msg["physmessage_id"]);
                if (!$physmessage_record) {
                    // Not found!
                    return FALSE;
                }

                /*
                 * Ok - prepare message data array
                 */
                $message_data = array(
                    'message_idnr' => $msg["message_idnr"],
                    'unique_id' => $msg["unique_id"],
                    'physmessage_id' => $msg['physmessage_id'],
                    'message_size' => $physmessage_record["messagesize"],
                    'seen_flag' => $msg["seen_flag"],
                    'answered_flag' => $msg["answered_flag"],
                    'deleted_flag' => $msg["deleted_flag"],
                    'flagged_flag' => $msg["flagged_flag"],
                    'folder_record' => array(
                        'name' => $mailboxes[$msg["mailbox_idnr"]]
                    ),
                    'mailbox_idnr' => $msg['mailbox_idnr']
                );

                /*
                 * Retrieve message headers / body
                 */
                $message = $this->retrieve_message($msg["message_idnr"], $mailboxes[$msg["mailbox_idnr"]], $message_data, FALSE);

                /*
                 * Format 'message-id' header according to 'dbmail_referencesfield' table content
                 */
                $message->thread_message_id = $this->clean_up_thread_message_id($message->messageID);

                /*
                 * Populate message_idnrs list
                 */
                $thread_messages_idnr[$msg["message_idnr"]] = $msg['physmessage_id'];

                /*
                 * Prevent duplicated items
                 */
                if (!array_key_exists($message->thread_message_id, $message_id_header_lookup)) {

                    /*
                     * Init base thread properties
                     */
                    $message->parent_uid = NULL;
                    $message->depth = 0;
                    $message->unread_children = 0;
                    $message->has_children = FALSE;

                    /*
                     * Add message to the list
                     */
                    $msg_index++;
                    $headers[$msg_index] = $message;

                    /*
                     * Set message-id header lookup
                     */
                    $message_id_header_lookup[$message->thread_message_id] = new stdClass();
                    $message_id_header_lookup[$message->thread_message_id]->msg_index = $msg_index;
                    $message_id_header_lookup[$message->thread_message_id]->message_idnr = $msg["message_idnr"];
                    $message_id_header_lookup[$message->thread_message_id]->physmessage_id = $msg['physmessage_id'];
                    $message_id_header_lookup[$message->thread_message_id]->folder = $mailboxes[$msg["mailbox_idnr"]];
                }
            }
        }

        /*
         * Here we set base properties needed to build a flat hierarchy starting 
         * from the conversation three (parent / depth / ...).
         */
        foreach ($headers as $msg_index => $message) {

            /*
             * Init threading properties (ONLY IF NOT ALREADY DONE!!!!!!).
             * Note that within inner 'foreach' we set parent messages properties 
             * too, so we need to avoid accidentally resetting them.
             */
            if (!isset($headers[$msg_index]->parent_uid) || strlen($headers[$msg_index]->parent_uid) == 0) {
                $headers[$msg_index]->parent_uid = NULL;
            }

            if (!isset($headers[$msg_index]->depth) || strlen($headers[$msg_index]->depth) == 0) {
                $headers[$msg_index]->depth = 0;
            }

            if (!isset($headers[$msg_index]->unread_children) || strlen($headers[$msg_index]->unread_children) == 0) {
                $headers[$msg_index]->unread_children = 0;
            }

            if (!isset($headers[$msg_index]->has_children) || $headers[$msg_index]->has_children !== TRUE) {
                $headers[$msg_index]->has_children = FALSE;
            }

            $physmessage_id = $message_id_header_lookup[$message->thread_message_id]->physmessage_id;
            $message_idnr = $message_id_header_lookup[$message->thread_message_id]->message_idnr;

            /*
             * Iterate over ancesters 
             */
            foreach ($message->ancesters as $ancester_ID) {

                $ancester = array_key_exists($ancester_ID, $message_id_header_lookup) ? $message_id_header_lookup[$ancester_ID] : FALSE;
                if (!$ancester) {
                    continue;
                }

                /*
                 *  update message properties
                 */
                $headers[$msg_index]->parent_uid = $ancester->message_idnr;
                $headers[$msg_index]->depth++;

                /*
                 *  update parent properties
                 */
                if (isset($headers[$msg_index]->flags['SEEN']) && $headers[$msg_index]->flags['SEEN'] === FALSE) {
                    $headers[$ancester->msg_index]->unread_children = isset($headers[$ancester->msg_index]->unread_children) && $headers[$ancester->msg_index]->unread_children > 0 ? $headers[$ancester->msg_index]->unread_children + 1 : 1;
                }

                if (!isset($headers[$ancester->msg_index]->has_children) || !$headers[$ancester->msg_index]->has_children) {
                    $headers[$ancester->msg_index]->has_children = TRUE;
                }
            }
        }

        /**
         * Translate messages internal-date property to timestamp so it will be easier to compare items by datetime
         */
        foreach ($headers as $msg_index => $message) {

            $internaldate = DateTime::createFromFormat('D, d M Y H:i:s O', $message->internaldate);
            if (!$internaldate) {
                $internaldate = new DateTime();
            }

            $headers[$msg_index]->internaldate_GMT = $internaldate->getTimestamp();
        }

        /**
         * Sort inner messages (those with depth greater than 0) by internal date, this will lead to:
         * - leave root messages ordered according to user selection
         * - list conveniently threaded messages
         */
        usort($headers, $this->_sort_threaded_messages_list());

        /**
         * Last but not the least... Here we recursivelly iterate over messages 
         * to build the flattern contents.
         */
        $response = array();
        $this->_flattern_threaded_messages($headers, NULL, $response);

        /**
         * Done!
         */
        return array_values($response);
    }

    /**
     * Decode supplied raw message using library PEAR Mail_mimeDecode
     * @param string $raw_message
     * @return Mail_mimeDecode
     */
    private function decode_raw_message($raw_message, $decode_bodies = TRUE) {

        /*
         * Cached content exists?
         */
//        $cache_key = "MIME_DECODED_MESSAGE_" . md5($raw_message);
//        $mime_decoded_message_cached = $this->get_cache($cache_key);
//        if (is_object($mime_decoded_message_cached)) {
//            return $mime_decoded_message_cached;
//        }

        $mime_decode = new Mail_mimeDecode($raw_message);

        $decode_params = array(
            'include_bodies' => TRUE,
            'decode_bodies' => $decode_bodies,
            'decode_headers' => TRUE,
            'rfc_822bodies' => TRUE
        );

        /*
         * add error suppression to avoid "Deprecated:  preg_replace(): The /e modifier 
         * is deprecated, use preg_replace_callback instead in 
         * ..../roundcube/vendor/pear-pear.php.net/Mail_mimeDecode/Mail/mimeDecode.php on line 762"
         */
        $decoded = @$mime_decode->decode($decode_params);

        /*
         *  add mime_id attributes to '$decoded' array items (pass by reference)
         */
        $mime_decode->getMimeNumbers($decoded);

        /*
         *  Store cached content
         */
//        $this->update_cache($cache_key, $decoded);

        return $decoded;
    }

    /**
     * create raw message from part lists
     * @param $physmessage_id
     * @param string $part Optional message part ID
     * @return stdClass
     */
    private function fetch_part_lists($physmessage_id, $part = NULL) {

        /*
         * Cached content exists?
         */
//        $cache_key = "RAW_MESSAGE_{$physmessage_id}_{$part}";
//        $raw_message_cached = $this->get_cache($cache_key);
//        if (is_object($raw_message_cached)) {
//            return $raw_message_cached;
//        }

        $query = " SELECT dbmail_partlists.part_depth, dbmail_partlists.is_header, dbmail_mimeparts.data "
                . " FROM dbmail_partlists "
                . " INNER JOIN dbmail_mimeparts on dbmail_partlists.part_id = dbmail_mimeparts.id "
                . " WHERE dbmail_partlists.physmessage_id = {$this->dbmail->escape($physmessage_id)} ";

        if (strlen($part) > 0) {
            $query .= " AND dbmail_partlists.part_id = {$this->dbmail->escape($part)} ";
        }

        $query .= " ORDER BY dbmail_partlists.part_key, dbmail_partlists.part_order ASC ";

        $result = $this->dbmail->query($query);

        $mimeParts = array();
        while ($row = $this->dbmail->fetch_assoc($result)) {
            $mimeParts[] = $row;
        }

        $depth = 0;
        $prevdepth = 0;
        $finalized = false;
        $is_header = true;
        $prev_header = true;
        $got_boundary = false;
        $prev_boundary = false;
        $prev_is_message = false;
        $is_message = false;
        $boundary = '';
        $blist = array();
        $index = 0;
        $header = '';
        $body = '';
        $newline = "\r\n";

        foreach ($mimeParts as $mimePart) {

            $depth = $mimePart['part_depth'];
            $is_header = $mimePart['is_header'];
            $blob = $mimePart['data'];

            if ($is_header) {
                $prev_boundary = $got_boundary;

                $is_message = preg_match('~content-type:\s+message/rfc822\b~i', $blob);
            }

            $got_boundary = false;

            $matches = array();
            //$pattern = '~^content-type:\s+.*;(\r?\n\s.*)*\s+boundary="?([a-z0-9\'()+_,-./:=\?\\s]*)"?~mi';
            $pattern = '~^content-type:\s+.*;(\r?\n\s.*)*\s+boundary="?([a-z0-9\'()+_,-./:=\?\s]*)"?~mi';
            if ($is_header && preg_match($pattern, $blob, $matches)) {
                list(,, $boundary) = $matches;
                $got_boundary = true;
                $blist[$depth] = trim($boundary);
            }

            /*
             * Code to handle the end of a mime part
             * 
             *  Testing if:
             *  - Previous part was initial part
             *  - This part is deeper than the previous (otherwise this part is finalized and the boundary is scrapped)
             *  - If a Boundary has been found
             */
            while (($prevdepth > 0) && ($prevdepth - 1 >= $depth) && $blist[$prevdepth - 1]) {
                $body .= $newline . "--" . $blist[$prevdepth - 1] . "--" . $newline;
                unset($blist[$prevdepth - 1]);
                $prevdepth--;
                $finalized = true;
            }

            if (($depth > 0) && (!empty($blist[$depth - 1]))) {
                $boundary = $blist[$depth - 1];
            }


            /*
             * Code to handle the end of the body
             */
            if ($is_header && (!$prev_header || $prev_boundary || ($prev_header && $depth > 0 && !$prev_is_message))) {
                //if ($is_header && (!$prev_header || $prev_boundary || ($prev_header && $depth > 0 && $prev_is_message))) {
                if ($prevdepth > 0) {
                    $body .= $newline;
                }
                $body .= "--" . $boundary . $newline;
            }

            /*
             * Let's handle what we have in the BLOB
             * 
             */
            if ($is_header && $depth == 0) {
                $header .= $blob;
            } else {
                $body .= $blob;
            }

            $body .= $newline;

            /*
             * Saving stuff for next iteration
             */
            $prevdepth = $depth;
            $prev_header = $is_header;
            $prev_is_message = $is_message;
            $index++;
        }

        if ($index > 2 && $boundary && !$finalized) {
            $body .= $newline . "--" . $boundary . "--" . $newline;
        }

        $response = new stdClass();
        $response->header = $header;
        $response->body = $body;

        /*
         *  Store cached content
         */
//        $this->update_cache($cache_key, $response);

        return $response;
    }

    private function get_structure($structure) {

        // merge headers to simplify searching by token
        $imploded_headers = '';
        foreach ($structure->headers as $header_name => $header_value) {
            $imploded_headers .= $header_name . $this->get_header_delimiter($header_name) . $header_value . "\n";
        }

        $charset = $this->get_header_value($imploded_headers, 'charset');
        $encoding = $this->get_header_value($imploded_headers, 'Content-Transfer-Encoding');
        $filename = $this->get_header_value($imploded_headers, 'filename');

        /*
         * Many mail user agents also send messages with the file name in the name parameter of 
         * the content-type header instead of the filename parameter of the content-disposition 
         * header. T
         * his practice is discouraged – the file name should be specified either through just 
         * the filename parameter, or through both the filename and the name parameters
         * 
         * https://en.wikipedia.org/wiki/MIME#Content-Disposition
         * 
         * https://tools.ietf.org/html/rfc2183
         */
        if (strlen($filename) == 0) {
            $filename = $this->get_header_value($imploded_headers, 'name');
        }

        $rcube_message_part = new rcube_message_part();
        $rcube_message_part->mime_id = (strlen($structure->mime_id) > 0 ? $structure->mime_id : 0);
        $rcube_message_part->ctype_primary = (strlen($structure->ctype_primary) > 0 ? $structure->ctype_primary : '');
        $rcube_message_part->ctype_secondary = (strlen($structure->ctype_secondary) > 0 ? $structure->ctype_secondary : '');
        $rcube_message_part->mimetype = "{$rcube_message_part->ctype_primary}/{$rcube_message_part->ctype_secondary}";
        $rcube_message_part->disposition = (strlen($structure->disposition) > 0 ? $structure->disposition : '');
        $rcube_message_part->filename = ($filename ? $filename : '');
        $rcube_message_part->encoding = ($encoding ? $encoding : '8bit');
        $rcube_message_part->charset = ($charset ? $charset : '');
        $rcube_message_part->size = (property_exists($structure, 'body') ? strlen($structure->body) : 0);
        $rcube_message_part->headers = (is_array($structure->headers) ? $structure->headers : array());
        $rcube_message_part->d_parameters = (is_array($structure->d_parameters) ? $structure->d_parameters : array());
        $rcube_message_part->ctype_parameters = (is_array($structure->ctype_parameters) ? $structure->ctype_parameters : array());


        if (property_exists($structure, 'parts')) {
            foreach ($structure->parts as $part) {
                $rcube_message_part->parts[] = $this->get_structure($part);
            }
        }

        return $rcube_message_part;
    }

    /**
     * Return mime part
     * @param stdClass $mime_decoded
     * @param string $mime_id
     * @return stdClass on success, False if not found
     */
    private function get_message_part_body($mime_decoded, $mime_id) {


        if (property_exists($mime_decoded, 'mime_id') && $mime_decoded->mime_id == $mime_id) {
            /*
             *  found
             */
            return (property_exists($mime_decoded, 'body') ? $mime_decoded->body : FALSE);
        }

        /*
         *  fetch children
         */
        if (property_exists($mime_decoded, 'parts') && is_array($mime_decoded->parts)) {

            foreach ($mime_decoded->parts as $part) {

                $body = $this->get_message_part_body($part, $mime_id);

                if ($body) {
                    /*
                     *  found
                     */
                    return $body;
                }
            }
        }

        /*
         * Nothing found
         */
        return FALSE;
    }

    /**
     * Set part_key on supplied part list items (nested) to treat it like a flat array
     * @param stdClass $mime_decoded
     * @return stdClass
     */
    private function set_part_keys(&$mime_decoded, $part_key) {

        if (property_exists($mime_decoded, 'headers')) {
            $mime_decoded->header_part_key = $part_key;
            $part_key++;
        }

        if (property_exists($mime_decoded, 'body')) {
            $mime_decoded->body_part_key = $part_key;
            $part_key++;
        }

        if (property_exists($mime_decoded, 'parts')) {

            foreach ($mime_decoded->parts as &$part) {

                $part_key = $this->set_part_keys($part, $part_key);
            }
        }

        return $part_key;
    }

    /**
     * return hashes string
     * @param string $string the source string
     * @return string the hashed string
     */
    private function hash_string($string) {

        $hashMethod = $this->rcubeInstance->config->get('dbmail_hash', "sha1");

        switch ($hashMethod) {
            case 'sha1':
                $hashed = hash("sha1", $string);
                break;
            case 'md5':
                $hashed = hash("md5", $string);
                break;
            case 'sha256':
                $hashed = hash("sha256", $string);
                break;
            case 'sha512':
                $hashed = hash("sha512", $string);
                break;
            case 'whirlpool':
                $hashed = hash("whirlpool", $string);
                break;
            case 'tiger':
                // don't know exactly which variant of tiger is the correct one
                $hashed = hash("tiger192,3", $string);
                break;
            default:
                // if everything fails... let's get back to a standard default
                $hashed = hash("sha1", $string);
                break;
        }

        return $hashed;
    }

    /**
     * Function to store part of a message (deduplication aware!!!!!!!)
     * 
     * @param int $physmessage_id
     * @param string $data the part to save
     * @param int $is_header is this part an header?
     * @param int $part_key part key
     * @param int $part_depth part depth
     * @param int $part_order part order
     * 
     * @return bool true on success, false on error
     */
    private function _part_insert($physmessage_id, $data, $is_header, $part_key, $part_depth, $part_order) {

        $hash = $this->hash_string($data);

        /*
         *  Enable debug as you please by changing class property
         */
        if ($this->debug === TRUE) {
            $this->rc->console("Part Insert, physmessage id: " . $physmessage_id);
            $this->rc->console("Part Insert, is header:      " . $is_header);
            $this->rc->console("Part Insert, part key:       " . $part_key);
            $this->rc->console("Part Insert, part depth:     " . $part_depth);
            $this->rc->console("Part Insert, part order:     " . $part_order);
            $this->rc->console("Part Insert, hash:           " . $hash);
        }

        /*
         *  blob exists?
         */
        $query = "SELECT id "
                . " FROM dbmail_mimeparts "
                . " WHERE hash = '{$this->dbmail->escape($hash)}' "
                . " AND size = {$this->dbmail->escape(strlen($data))}";

        $result = $this->dbmail->query($query);

        if ($this->dbmail->num_rows($result) == 0) {

            /*
             *  blob not found - insert new record in 'dbmail_mimeparts'
             */
            $query = "INSERT INTO dbmail_mimeparts "
                    . " ( "
                    . "    hash, "
                    . "    data, "
                    . "    size "
                    . " ) "
                    . " VALUES "
                    . " ( "
                    . "    '{$this->dbmail->escape($hash)}', "
                    . "    '{$this->dbmail->escape($data)}', "
                    . "    {$this->dbmail->escape(strlen($data))} "
                    . " ) ";

            if (!$this->dbmail->query($query)) {
                return FALSE;
            }

            /*
             *  retrieve inserted ID
             */
            $part_id = $this->dbmail->insert_id('dbmail_mimeparts');
            if (!$part_id) {
                return FALSE;
            }
        } else {
            /*
             *  blob found - use current record ID
             */
            $row = $this->dbmail->fetch_assoc($result);
            $part_id = $row["id"];
        }

        /*
         *  register 'dbmail_partlists' to 'dbmail_mimeparts' relation
         */
        $query = "INSERT INTO dbmail_partlists "
                . " ( "
                . "    physmessage_id, "
                . "    is_header, "
                . "    part_key, "
                . "    part_depth, "
                . "    part_order, "
                . "    part_id "
                . " ) "
                . " VALUES "
                . " ( "
                . "    '{$this->dbmail->escape($physmessage_id)}', "
                . "    '{$this->dbmail->escape($is_header)}', "
                . "    '{$this->dbmail->escape($part_key)}', "
                . "    '{$this->dbmail->escape($part_depth)}', "
                . "    '{$this->dbmail->escape($part_order)}', "
                . "    '{$this->dbmail->escape($part_id)}' "
                . " )";

        return ($this->dbmail->query($query) ? TRUE : FALSE);
    }

    /**
     * Function to extract the RAW Headers of a message
     * It simply read the message up to the first empty new line
     * Partially stolen from https://github.com/plancake/official-library-php-email-parser
     * 
     * NOTE: PEAR Mail_mimeDecode returns lowercased headers name, so we use 
     * this method to get real message headers
     *
     * @param string $input the message to parse
     *
     * @return string $raw_header the message headers
     */
    private function extract_raw_headers_from_message($input) {

        $lines = preg_split("/(\r?\n|\r)/", $input);

        $raw_header = '';

        foreach ($lines as $line) {

            if (strlen(trim($line)) == 0) {
                /*
                 * Done!
                 */
                break;
            }

            $raw_header .= $line . "\n";
        }

        return $raw_header;
    }

    /**
     * Store supplied part item
     * @param int $physmessage_id
     * @param Mail_mimeDecode obj
     * @param int $part_key (1 = first item) - by refereeence!
     * @param int $part_depth (0 = first item)
     * @param int $part_order (0 = first item)
     * @return boolean True on success, False on Failure
     */
    private function store_mime_object($physmessage_id, $mime_decoded, &$part_key, $part_depth, $part_order) {

        /*
         *  Enable debug as you please by changing class property
         */
        if ($this->debug === TRUE) {
            $this->rc->console("Store mime object");
            $this->rc->console($mime_decoded);
            $this->rc->console("Store mime object - part key:   " . $part_key);
            $this->rc->console("Store mime object - part depth: " . $part_depth);
            $this->rc->console("Store mime object - part order: " . $part_order);
        }

        /*
         *  Top level headers (depth = 0) are taken directly from the message envelope
         */
        if ($part_depth > 0 && property_exists($mime_decoded, 'headers')) {

            $part_key++;

            $headers = '';
            foreach ($mime_decoded->headers as $header_name => $header_value) {

                /*
                 *  Headers have a specific CASE-matching rule...
                 */
                if (strtolower($header_name) == "content-type") {
                    $header_name = "Content-Type";
                } elseif (strtolower($header_name) == "mime-version") {
                    $header_name = "MIME-Version";
                }
                $headers .= $header_name . $this->get_header_delimiter($header_name) . $header_value . "\n";
            }

            if (!$this->_part_insert($physmessage_id, $headers, 1, $part_key, $part_depth, $part_order)) {
                return FALSE;
            }

            $part_order++;
        }

        /*
         *  Do we have a body?
         */
        if (property_exists($mime_decoded, 'body')) {
            //$this->rc->console("We have a message");
            if (!$this->_part_insert($physmessage_id, $mime_decoded->body, 0, $part_key, $part_depth, $part_order)) {
                return FALSE;
            }
        } elseif ($part_depth == 0) {
            //$this->rc->console("Empty body for first level");
            if (!$this->_part_insert($physmessage_id, "This is a multi-part message in MIME format.", 0, $part_key, $part_depth, $part_order)) {
                return FALSE;
            }
        }

        /*
         *  Do we have additional parts?
         */
        if (property_exists($mime_decoded, 'parts') && is_array($mime_decoded->parts)) {
            //$this->rc->console("We have parts");

            $part_depth++;
            $part_order = 0;  // When depth rises, order goes zero

            foreach ($mime_decoded->parts as $eachPart) {

                if (!$this->store_mime_object($physmessage_id, $eachPart, $part_key, $part_depth, $part_order)) {
                    return FALSE;
                }

                $part_order++;
            }
        }

        return TRUE;
    }

    /**
     * Increment user quota
     * @param int $user_idnr user identifier
     * @param int $size the size to decrement from user quota
     * @return boolean True on success, False on failure
     */
    private function increment_user_quota($user_idnr, $size) {

        $query = "UPDATE dbmail_users "
                . " SET curmail_size = (curmail_size + {$this->dbmail->escape($size)}) "
                . " WHERE user_idnr = {$this->dbmail->escape($user_idnr)} ";

        return ($this->dbmail->query($query) ? TRUE : FALSE);
    }

    /**
     * Decrement user quota
     * @param int $user_idnr user identifier
     * @param int $size the size to decrement from user quota
     * @return boolean True on success, False on failure
     */
    private function decrement_user_quota($user_idnr, $size) {

        $query = "UPDATE dbmail_users "
                . " SET curmail_size = "
                . "    CASE "
                . "       WHEN curmail_size >= {$this->dbmail->escape($size)} "
                . "          THEN ( curmail_size - {$this->dbmail->escape($size)} ) "
                . "       ELSE 0 "
                . "    END "
                . " WHERE user_idnr = {$this->dbmail->escape($user_idnr)} ";

        return ($this->dbmail->query($query) ? TRUE : FALSE);
    }

    /**
     * Retrieve searchable headers key / pairs
     * @param string $raw_headers raw headers
     * @return array
     */
    private function get_searchable_headers($raw_headers) {

        $searchable_headers = array();

        foreach ($this->searchable_headers as $header_name) {

            $header_value = $this->get_header_value($raw_headers, $header_name);

            if ($header_value) {
                $searchable_headers[$header_name] = $header_value;
            }
        }

        return $searchable_headers;
    }

    /**
     * Store searchable header name / value pair
     * @param int $physmessage_id
     * @param string $header_name
     * @param string $header_value
     * @param boolean True on success, False on failure
     */
    private function save_searchable_header($physmessage_id, $header_name, $header_value) {

        /*
         *  add new header names to 'dbmail_headername' table?
         */
        $dbmail_fixed_headername_cache = $this->rcubeInstance->config->get('dbmail_fixed_headername_cache', null);

        /*
         *  retrieve $header_name_id (if exists)
         */
        $header_name_id = $this->get_header_id_by_header_name($header_name);

        /*
         *  retrieve $header_value_id (if exists)
         */
        $header_value_id = $this->get_header_value_id_by_header_value($header_value);

        if (!$dbmail_fixed_headername_cache && !$header_name_id) {
            /*
             *  header name doesn't exists and we don't want to add extra headers - OK
             */
            return TRUE;
        }

        // fix missing header_name reference (if needed)
        if (!$header_name_id) {

            /*
             *  header name doesn't exists - create it
             */
            $headerNameSQL = "INSERT INTO dbmail_headername "
                    . " ( headername )"
                    . " VALUES "
                    . " ( '{$this->dbmail->escape($header_name)}' )";


            if (!$this->dbmail->query($headerNameSQL)) {
                return FALSE;
            }

            // retrieve inserted ID
            $header_name_id = $this->dbmail->insert_id('dbmail_headername');
            if (!$header_name_id) {
                return FALSE;
            }
        }

        /*
         *  fix missing header_value reference (if needed)
         */
        if (!$header_value_id) {

            $date = DateTime::createFromFormat('Y-m-d H:i:s', $header_value);
            $escaped_date_field = ($date ? "'{$this->dbmail->escape($date->format('Y-m-d H:i:s'))}'" : "NULL");

            if ($header_name == "date") {
                $dt = new DateTime($this->dbmail->escape($header_value));

                $dt->setTimezone(new DateTimeZone('GMT'));
                $sortfield = $dt->format('Y-m-d H:i:s');
                $escaped_date_field = "'" . $dt->format('Y-m-d 00:00:00') . "'";
            } else {
                $sortfield = substr($this->dbmail->escape($header_value), 0, 254);
            }

            // header value doesn't exists - create it
            $headerValueSQL = "INSERT INTO dbmail_headervalue "
                    . " ( "
                    . "    hash, "
                    . "    headervalue, "
                    . "    sortfield, "
                    . "    datefield "
                    . " )"
                    . " VALUES "
                    . " ( "
                    . "    '{$this->dbmail->escape($this->hash_string($header_value))}', "
                    . "    '{$this->dbmail->escape($header_value)}', "
                    . "    '{$sortfield}', "
                    . "     {$escaped_date_field} "
                    . " )";


            if (!$this->dbmail->query($headerValueSQL)) {
                return FALSE;
            }

            // retrieve inserted ID
            $header_value_id = $this->dbmail->insert_id('dbmail_headervalue');
            if (!$header_value_id) {
                return FALSE;
            }
        }

        /*
         *  add dbmail_headername to dbmail_headervalue relation
         */
        $query = "INSERT INTO dbmail_header "
                . " ( "
                . "    physmessage_id, "
                . "    headername_id, "
                . "    headervalue_id "
                . " ) "
                . " VALUES "
                . " ( "
                . "    '{$this->dbmail->escape($physmessage_id)}', "
                . "    '{$this->dbmail->escape($header_name_id)}', "
                . "    '{$this->dbmail->escape($header_value_id)}' "
                . " )";

        return ($this->dbmail->query($query) ? TRUE : FALSE);
    }

    /**
     * Retrieve mail envelope headers
     * @param string $raw_headers raw headers
     * @return string formatted dbmail envelope headers
     */
    private function get_mail_envelope_headers($raw_headers) {

        /* date */
        /* subject */
        /* from */
        /* sender */
        /* reply-to */
        /* to */
        /* cc */
        /* bcc */
        /* in-reply-to */
        /* message-id */

        $date = $this->get_header_value($raw_headers, 'date');
        // format 'date'
        $date = ($date ? '"' . $date . '"' : 'NIL');

        $subject = $this->get_header_value($raw_headers, 'subject');
        // format 'date'
        $subject = ($subject ? '"' . $subject . '"' : 'NIL');

        $from = $this->decode_mail_envelope_address($this->get_header_value($raw_headers, 'from'));
        $sender = $this->decode_mail_envelope_address($this->get_header_value($raw_headers, 'x-sender'));
        $reply_to = $this->decode_mail_envelope_address($this->get_header_value($raw_headers, 'reply-to'));

        $to = $this->decode_mail_envelope_address($this->get_header_value($raw_headers, 'to'));
        $cc = $this->decode_mail_envelope_address($this->get_header_value($raw_headers, 'cc'));
        $bcc = $this->decode_mail_envelope_address($this->get_header_value($raw_headers, 'bcc'));
        $in_reply_to = $this->decode_mail_envelope_address($this->get_header_value($raw_headers, 'in-reply-to'));

        $message_id = $this->get_header_value($raw_headers, 'message-id');
        // format '$message_id'
        $message_id = ($message_id ? '"' . $message_id . '"' : 'NIL');


        $envelope_headers = "(";
        $envelope_headers .= "{$date} ";
        $envelope_headers .= "{$subject} ";
        $envelope_headers .= "{$from} ";
        $envelope_headers .= "{$sender} ";
        $envelope_headers .= "{$reply_to} ";
        $envelope_headers .= "{$to} ";
        $envelope_headers .= "{$cc} ";
        $envelope_headers .= "{$bcc} ";
        $envelope_headers .= "{$in_reply_to} ";
        $envelope_headers .= "{$message_id}";
        $envelope_headers .= ")";

        return $envelope_headers;
    }

    /**
     * Format supplied mail address according to dbmail envelope structure
     */
    private function decode_mail_envelope_address($address) {

        $decoded_address = '';

        if (strlen($address) > 0) {

            // '$address' could be 'maxpower@homer.com' or 'Max Power <maxpower@homer.com>' or FALSE if not found

            $pos = strpos($address, '<');

            if ($pos !== false) {

                // alias found - split alias, user and domain

                $alias = substr($address, 0, ($pos - 1));
                $address = substr($address, ($pos + 1), (strlen($address) - 2));

                list($user, $domain) = explode('@', $address);

                $decoded_address = '(("' . $alias . '" NIL "' . $user . '" "' . $domain . '"))';
            } else {

                // alias not found - split user and domain

                list($user, $domain) = explode('@', $address);

                $decoded_address = '((NIL NIL "' . $user . '" "' . $domain . '"))';
            }
        } else {
            // nothing found
            $decoded_address = 'NIL';
        }

        return $decoded_address;
    }

    /**
     * Store mail evelope headers
     * @param int $physmessage_id
     * @param string formatted dbmail envelope headers
     * @return boolean True on success, False on failure
     */
    private function save_mail_envelope_headers($physmessage_id, $envelope_headers) {

        if (!$this->get_physmessage_record($physmessage_id) || strlen($envelope_headers) == 0) {
            /*
             * Invalid parameters suplied
             */
            return FALSE;
        }

        $query = "INSERT INTO dbmail_envelope "
                . " ( "
                . "    physmessage_id, "
                . "    envelope "
                . " )"
                . " VALUES "
                . " ( "
                . "    '{$this->dbmail->escape($physmessage_id)}', "
                . "    '{$this->dbmail->escape($envelope_headers)}' "
                . " ) ";

        return ($this->dbmail->query($query) ? TRUE : FALSE);
    }

    /**
     * Stores folder statistic data in session
     *
     * @param string $folder  Folder name
     * @param string $name    Data name
     * @param mixed  $data    Data value
     */
    protected function set_folder_stats($folder, $name, $data) {

        $_SESSION['folders'][$folder][$name] = $data;
    }

    /**
     * Gets folder statistic data
     *
     * @param string $folder Folder name
     *
     * @return array Stats data
     */
    protected function get_folder_stats($folder) {

        return ($_SESSION['folders'][$folder] ? (array) $_SESSION['folders'][$folder] : array());
    }

    /* --------------------------------
     * Caching methods
     * --------------------------------
     * 
     * We don't differentiate between caching types
     * as we basically always use a key->value cache
     * 
     */

    /**
     * Enable or disable GENERAL cache
     *
     * @param string $type Cache type (@see rcube::get_cache)
     */
    public function set_caching($type) {

        if ($type) {
            $this->caching = $type;
        } else {
            if ($this->cache) {
                $this->cache->close();
            }
            $this->cache = null;
            $this->caching = false;
        }
    }

    /**
     * Common initialization for the cache engine
     */
    protected function get_cache_engine() {

        if ($this->caching && !$this->cache) {
            $rcube = rcube::get_instance();
            $ttl = $rcube->config->get('dbmail_cache_ttl', '10d');
            $this->cache = $rcube->get_cache('DBMAIL', $this->caching, $ttl);
        }

        return $this->cache;
    }

    /**
     * Returns cached value
     *
     * @param string $key Cache key
     *
     * @return mixed
     */
    public function get_cache($key) {

        $cache = $this->get_cache_engine();

        return ($cache ? $cache->get($key) : NULL);
    }

    /**
     * Update cache
     *
     * @param string $key  Cache key
     * @param mixed  $data Data
     */
    public function update_cache($key, $data) {

        $cache = $this->get_cache_engine();

        if ($cache) {
            $cache->set($key, $data);
        }
    }

    /**
     * Clears the cache.
     *
     * @param string  $key         Cache key name or pattern
     * @param boolean $prefix_mode Enable it to clear all keys starting
     *                             with prefix specified in $key
     */
    public function clear_cache($key = null, $prefix_mode = false) {

        $cache = $this->get_cache_engine();

        if ($cache) {
            $cache->remove($key, $prefix_mode);
        }
    }

    /**
     * Enable or disable messages caching
     *
     * @param boolean $set  Flag
     * @param int     $mode Cache mode
     */
    public function set_messages_caching($set, $mode = null) {
        /*
         *  Not used as we rely on general cache 
         */
    }

    /* --------------------------------
     * Helper methods
     * --------------------------------
     * 
     */

    /**
     * 'usort' callback method to sort objects list on supplied property name
     * @param string $key property name
     * @param string $order (ASC / DESC)
     * @example usort($myObjectsList, $this->multidimensionalObjsArraySort('propertyName', 'ASC'));
     */
    private function multidimensionalObjsArraySort($key, $order) {

        if (strtoupper($order) == 'ASC') {

            return function ($a, $b) use ($key) {
                return strnatcmp($a->$key, $b->$key);
            };
        } else {

            return function ($b, $a) use ($key) {
                return strnatcmp($a->$key, $b->$key);
            };
        }
    }

    /**
     * 'usort' callback method to sort multidimensional array on supplied key
     * @param string $key array key
     * @param string $order (ASC / DESC)
     * @example usort($myArray, $this->multidimensionalArraySort('keyName', 'ASC'));
     */
    private function multidimensionalArraySort($key, $order) {

        if (strtoupper($order) == 'ASC') {

            return function ($a, $b) use ($key) {
                return strnatcmp($a[$key], $b[$key]);
            };
        } else {

            return function ($b, $a) use ($key) {
                return strnatcmp($a[$key], $b[$key]);
            };
        }
    }

    /**
     * Temporary save supplied content
     * @param string $key
     * @param mixed $content
     * @param int $TTL
     * @return boolean
     */
    private function set_temp_value($key = '', $content = '', $TTL = NULL) {


        if (strlen($TTL) == 0) {
            /*
             * Set default TTL if none supplied
             */
            $TTL = self::TEMP_TTL;
        }

        /*
         * Set cache expire TS
         */
        $expiresAt = time() + $TTL;


        /*
         * We store TMP data within current session; feel free to move to another 'storage' target if you prefer (memcached, apc, ...)
         */
        $_SESSION[$key] = array(
            'expiresAt' => $expiresAt,
            'content' => $content
        );

        return TRUE;
    }

    /**
     * Retrieve stored temporary content
     * @param string $key
     * @return mixed value on success, FALSE on failure (key not found / expired)
     */
    private function get_temp_value($key = '') {

        if (is_array($_SESSION) && array_key_exists($key, $_SESSION) && array_key_exists('expiresAt', $_SESSION[$key]) && time() <= $_SESSION[$key]['expiresAt']) {
            return $_SESSION[$key]['content'];
        } else {
            return FALSE;
        }
    }

    /**
     * Delete stored temporary content
     * @param string $key
     * @return boolean
     */
    private function unset_temp_value($key = '') {

        if (is_array($_SESSION) && array_key_exists($key, $_SESSION)) {
            unset($_SESSION[$key]);
        }

        return TRUE;
    }

    private function _format_folders_list($folder) {

        $folders = array();

        if (is_array($folder) && count($folder) > 0) {
            $folders = $folder;
        } elseif (is_string($folder) && strlen(trim($folder)) > 0) {
            $folders = array(trim($folder));
        } else {
            $folders = array($this->folder);
        }

        return array_unique($folders);
    }

    /**
     * Translate search filters
     * @param string $search_string
     * @return array formatted search parameters
     */
    private function _translate_search_parameters($search_string = '') {

        /*
         * Search parameters are supplied in polish notation and could contain nested blocks:
         * eg:
         *      - OR SUBJECT "aaaa"  (OR HEADER FROM "bbbb"  (OR HEADER TO "cccc"  (OR CC "dddd" BCC "eeee")))
         *      - OR SUBJECT "aaaa" HEADER FROM "bbbb" OR HEADER TO "cccc" CC "dddd" ALL BCC "5555" 
         *      - OR OR OR OR OR HEADER SUBJECT 123456 HEADER FROM 123456 HEADER TO 123456 HEADER CC 123456 HEADER BCC 123456 BODY 123456
         *      - ALL SUBJECT "aaaa" ALL HEADER FROM "bbbb" ALL HEADER TO "cccc" ALL CC "dddd" ALL BCC "5555"
         *      - ALL
         */

        /*
         * Init response container
         */
        $search_conditions = new stdClass();
        $search_conditions->additional_joins = array();
        $search_conditions->additional_where_conditions = '';
        $search_conditions->needs_physmessages = FALSE;

        if (strlen($search_string) == 0) {
            /*
             * Empry set
             */
            return $search_conditions;
        }

        /*
         * Init sequence container (to produce unique table aliases)
         */
        $sequence = 0;

        /*
         * Init placeholders container
         */
        $placeholders = array();
        $placeholder_index = 0;

        /*
         * Strip unnecessary whitespaces from search string
         */
        $search_string = preg_replace('/\s+/', ' ', trim($search_string));

        /*
         * Wrap supplied $search_string 
         */
        $search_string = '(' . $search_string . ')';

        while (strrpos($search_string, '(') !== FALSE) {

            /*
             * We start from last parenthesised condition. Let's retrieve last open parenthesis position
             */
            $opening_parenthesis_position = strrpos($search_string, '(');

            /*
             * Retrieve ollowing closing parenthesis position
             */
            $closing_parenthesis_position = strpos($search_string, ')', $opening_parenthesis_position);
            if ($closing_parenthesis_position === FALSE) {
                return FALSE;
            }

            /*
             * Extract parenthesised search criteria
             */
            $parenthesised_search_criteria = substr($search_string, ($opening_parenthesis_position + 1), ($closing_parenthesis_position - $opening_parenthesis_position - 1));

            /*
             * Tokenize search criteria
             */
            $items = $this->_tokenize_string($parenthesised_search_criteria);

            /*
             * Step 1: split 'ANDed' segments ('ALL' keyword)
             */
            $segments = array();
            $segment_index = 0;

            foreach ($items as $item) {

                if ($item == 'ALL') {
                    $segment_index++;
                    continue;
                }

                $segments[$segment_index][] = $item;
            }

            /*
             * Init temporary containers
             */
            $additional_where_conditions = '';

            /*
             * Step 2: fetch splitted segments to manage 'ORed' conditions
             */
            foreach ($segments as $segment_index => $segment) {

                /*
                 * At this point, within the segment all operands are 'ORed'. So we can ingore operators and directly work on operands.
                 */
                $ORed_conditions = 0;
                while (in_array('OR', $segment) !== FALSE) {
                    $position = array_search('OR', $segment);
                    unset($segment[$position]);
                    $ORed_conditions++;
                }

                $segment = array_values($segment);

                $segment_where_conditions = '';

                while (count($segment) > 0) {

                    /*
                     * Placeholder replacement?
                     */
                    if (substr($segment[0], 0, 15) == '###PLACEHOLDER_' && array_key_exists($segment[0], $placeholders)) {

                        /*
                         * Placeholder found!
                         */
                        $segment_where_conditions[] = " ( {$placeholders[$segment[0]]} ) ";

                        unset($placeholders[$segment[0]]);
                        unset($segment[0]);

                        $segment = array_values($segment);

                        continue;
                    }


                    $sql_structure = $this->_apply_search_parameters($segment, $sequence);
                    if ($sql_structure === FALSE) {
                        return FALSE;
                    }

                    /*
                     * Update  temporary containers
                     */
                    foreach ($sql_structure->additional_joins as $additional_join) {
                        $search_conditions->additional_joins[] = $additional_join;
                    }

                    $segment_where_conditions[] = " ( {$sql_structure->additional_where_conditions} ) ";

                    if ($sql_structure->needs_physmessages) {
                        $search_conditions->needs_physmessages = TRUE;
                    }
                }

                if (strlen($additional_where_conditions) > 0) {
                    $additional_where_conditions .= " AND ";
                }

                $additional_where_conditions .= " ( " . implode(' OR ', $segment_where_conditions) . " ) ";
            }

            /*
             * Set placeholder
             */
            $placeholder_index++;
            $placeholder = "###PLACEHOLDER_{$placeholder_index}###";
            $placeholders[$placeholder] = $additional_where_conditions;


            /*
             * Replace content with target placeholder
             */
            $search_string = substr_replace($search_string, $placeholder, $opening_parenthesis_position, ($closing_parenthesis_position - $opening_parenthesis_position + 1));
        }


        /*
         * At this point, $placeholders list must contain only 1 item
         */
        if (count($placeholders) != 1) {
            return FALSE;
        }

        /*
         * Ok - Extract 'additional_where_conditions' from $placeholders list
         */
        $search_conditions->additional_where_conditions = array_pop($placeholders);

        return $search_conditions;
    }

    /**
     * Parse IMAP search parameters into an array
     * @param string $string IMAP search string
     * @return array tokens
     */
    private function _tokenize_string($string = '') {

        $arguments_delimiter = '"';
        $escape_sign = '\\';

        /*
         * Init stack
         */
        $tokens = array(
            0 => ''
        );

        /*
         * Keep track of quoted strings
         */
        $is_quoted = FALSE;

        /*
         * Fetch 1 char at time from input string
         */
        $string = trim($string);
        $chars = str_split($string);

        foreach ($chars as $index => $char) {

            /*
             * Get latest stack token index
             */
            $token_index = count($tokens) - 1;

            /*
             * Get previous and following chars
             */
            $previous_char = array_key_exists($index - 1, $chars) ? $chars[$index - 1] : '';
            $following_char = array_key_exists($index + 1, $chars) ? $chars[$index + 1] : '';

            if (!$is_quoted && $char == $arguments_delimiter && $previous_char == " ") {
                /*
                 * Opening quote found
                 */
                $is_quoted = TRUE;
                continue;
            }

            if ($is_quoted && $char == $escape_sign && $following_char == $arguments_delimiter) {

                /*
                 * Escaped sign found
                 */
                //$tokens[$token_index] .= $char;
                continue;
            }

            if ($is_quoted && $char == $arguments_delimiter && $previous_char == $escape_sign) {

                /*
                 * Escaped quote found
                 */
                $tokens[$token_index] .= $char;
                continue;
            }

            if ($is_quoted && $char == $arguments_delimiter && $previous_char != $escape_sign) {

                /*
                 * Closing quote found
                 */
                $is_quoted = FALSE;
                continue;
            }

            if ($char != ' ' || $is_quoted) {
                /*
                 * Whitespaces delimit tokens (only when not within quoted string).
                 * Push char into latest stack token
                 */
                $tokens[$token_index] .= $char;
                continue;
            }

            /*
             * Add one more stack token
             */
            $token_index++;
            $tokens[$token_index] = '';
        }

        return $tokens;
    }

    private function _cleanup_search_parameter($string = '') {

        // remove leading / trailing double quotes 
        if (strlen($string) > 2 && substr($string, 0) == '"' && substr($string, -1) == '"') {
            $string = trim($string, '"');
        }

        return $string;
    }

    /**
     * Apply supplied search parameters.
     * @param array $items tokenized IMAP search string (passed by reference to directly remove used tokens)
     * @param int $sequence
     * @return array matching messages IDs on success, FALSE on failure
     */
    private function _apply_search_parameters(&$items = array(), &$sequence = 0) {

        /*
         * Valid input?
         */
        if (!is_array($items) || !array_key_exists(0, $items)) {
            return FALSE;
        }

        /*
         * Init response container
         */
        $search_conditions = new stdClass();
        $search_conditions->additional_joins = array();
        $search_conditions->additional_where_conditions = '';
        $search_conditions->needs_physmessages = FALSE;

        /*
         * Not keyword supplied? (reverse search)
         */
        $is_not = FALSE;
        if ($items[0] == 'NOT') {

            $is_not = TRUE;

            /*
             * Remove 'NOT' clause from operator
             */
            $items = array_slice($items, 1);
        }


        /*
         * Operators management
         */
        switch ($items[0]) {
            case 'ANSWERED':

                /*
                  ANSWERED
                  Messages with the \Answered flag set.
                 */
                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.answered_flag = 1";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.answered_flag <> 1";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'BCC':

                /*
                  BCC <string>
                  Messages that contain the specified string in the envelope
                  structure's BCC field.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $header_name_id = $this->get_header_id_by_header_name('bcc');
                if (!$header_name_id) {
                    /*
                     * Missing lookup value - return empty string (this is NON an error!)
                     */
                    return array();
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_header_alias = "dbmail_header_{$sequence}";

                $sequence++;
                $dbmail_headervalue_alias = "dbmail_headervalue_{$sequence}";

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_header AS {$dbmail_header_alias} ON dbmail_physmessage.id = {$dbmail_header_alias}.physmessage_id",
                    "INNER JOIN dbmail_headervalue AS {$dbmail_headervalue_alias} ON {$dbmail_header_alias}.headervalue_id = {$dbmail_headervalue_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue LIKE '%{$this->dbmail->escape($search_value)}%'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue NOT LIKE '%{$this->dbmail->escape($search_value)}%'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'BEFORE':

                /*
                  BEFORE <date>
                  Messages whose internal date (disregarding time and timezone)
                  is earlier than the specified date.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                $datetime_value = date_create_from_format('j-M-Y', $search_value);
                if (!$datetime_value) {
                    /*
                     * Malformed datetime
                     */
                    return FALSE;
                }

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.internal_date < '{$datetime_value->format('Y-m-d')} 00:00:00'";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.internal_date >= '{$datetime_value->format('Y-m-d')} 00:00:00'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'BODY':

                /*
                  BODY <string>
                  Messages that contain the specified string in the body of the
                  message.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_partlists_alias = "dbmail_partlists_{$sequence}";

                $sequence++;
                $dbmail_mimeparts_alias = "dbmail_mimeparts_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_partlists AS  {$dbmail_partlists_alias} ON dbmail_physmessage.id = {$dbmail_partlists_alias}.physmessage_id",
                    "INNER JOIN dbmail_mimeparts AS {$dbmail_mimeparts_alias} ON {$dbmail_partlists_alias}.part_id = {$dbmail_mimeparts_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_partlists_alias}.is_header = 0 AND {$dbmail_mimeparts_alias}.data LIKE '%{$this->dbmail->escape($search_value)}%'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_partlists_alias}.is_header = 0 AND {$dbmail_mimeparts_alias}.data NOT LIKE '%{$this->dbmail->escape($search_value)}%'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'CC':

                /*
                  CC <string>
                  Messages that contain the specified string in the envelope
                  structure's CC field.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $header_name_id = $this->get_header_id_by_header_name('cc');
                if (!$header_name_id) {
                    /*
                     * Missing lookup value - return empty string (this is NON an error!)
                     */
                    return array();
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_header_alias = "dbmail_header_{$sequence}";

                $sequence++;
                $dbmail_headervalue_alias = "dbmail_headervalue_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_header AS {$dbmail_header_alias} ON dbmail_physmessage.id = {$dbmail_header_alias}.physmessage_id",
                    "INNER JOIN dbmail_headervalue AS {$dbmail_headervalue_alias} ON {$dbmail_header_alias}.headervalue_id = {$dbmail_headervalue_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue LIKE '%{$this->dbmail->escape($search_value)}%'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue NOT LIKE '%{$this->dbmail->escape($search_value)}%'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'DELETED':

                /*
                  DELETED
                  Messages with the \Deleted flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.deleted_flag = 1";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.deleted_flag <> 1";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'DRAFT':

                /*
                  DRAFT
                  Messages with the \Draft flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.draft_flag = 1";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.draft_flag <> 1";
                }

                $items = array_slice($items, 1);

                break;
            case 'FLAGGED':

                /*
                  FLAGGED
                  Messages with the \Flagged flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.flagged_flag = 1";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.flagged_flag <> 1";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'FROM':

                /*
                  FROM <string>
                  Messages that contain the specified string in the envelope
                  structure's FROM field.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $header_name_id = $this->get_header_id_by_header_name('from');
                if (!$header_name_id) {
                    /*
                     * Missing lookup value - return empty string (this is NON an error!)
                     */
                    return array();
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_header_alias = "dbmail_header_{$sequence}";

                $sequence++;
                $dbmail_headervalue_alias = "dbmail_headervalue_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_header AS {$dbmail_header_alias} ON dbmail_physmessage.id = {$dbmail_header_alias}.physmessage_id",
                    "INNER JOIN dbmail_headervalue AS {$dbmail_headervalue_alias} ON {$dbmail_header_alias}.headervalue_id = {$dbmail_headervalue_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue LIKE '%{$this->dbmail->escape($search_value)}%'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue NOT LIKE '%{$this->dbmail->escape($search_value)}%'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'HEADER':

                /*
                  HEADER <field-name> <string>
                  Messages that have a header with the specified field-name (as
                  defined in [RFC-2822]) and that contains the specified string
                  in the text of the header (what comes after the colon).  If the
                  string to search is zero-length, this matches all messages that
                  have a header line with the specified field-name regardless of
                  the contents.
                 */

                if (!array_key_exists(1, $items) || !array_key_exists(2, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $header_name_id = $this->get_header_id_by_header_name(strtolower($items[1]));
                if (!$header_name_id) {
                    /*
                     * Missing lookup value - return empty string (this is NON an error!)
                     */
                    return array();
                }

                $search_value = $this->_cleanup_search_parameter($items[2]);

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_header_alias = "dbmail_header_{$sequence}";

                $sequence++;
                $dbmail_headervalue_alias = "dbmail_headervalue_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_header AS {$dbmail_header_alias} ON dbmail_physmessage.id = {$dbmail_header_alias}.physmessage_id",
                    "INNER JOIN dbmail_headervalue AS {$dbmail_headervalue_alias} ON {$dbmail_header_alias}.headervalue_id = {$dbmail_headervalue_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue LIKE '%{$this->dbmail->escape($search_value)}%'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue NOT LIKE '%{$this->dbmail->escape($search_value)}%'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 3);

                break;
            case 'KEYWORD':

                /*
                  KEYWORD <flag>
                  Messages with the specified keyword flag set.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_keywords_alias = "dbmail_keywords{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_keywords AS {$dbmail_keywords_alias} on dbmail_messages.message_idnr = {$dbmail_keywords_alias}.message_idnr"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_keywords_alias}.keyword = '{$this->dbmail->escape($search_value)}'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_keywords_alias}.keyword <> '{$this->dbmail->escape($search_value)}'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'LARGER':

                /*
                  LARGER <n>
                  Messages with an [RFC-2822] size larger than the specified
                  number of octets.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.rfcsize > '{$this->dbmail->escape($search_value)}'";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.rfcsize <= '{$this->dbmail->escape($search_value)}'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'NEW':

                /*
                  NEW
                  Messages that have the \Recent flag set but not the \Seen flag.
                  This is functionally equivalent to "(RECENT UNSEEN)".
                 */
                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.recent_flag = 1 AND dbmail_messages.seen_flag = 0";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.recent_flag = 1 AND dbmail_messages.seen_flag <> 0";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'OLD':

                /*
                  OLD
                  Messages that do not have the \Recent flag set.  This is
                  functionally equivalent to "NOT RECENT" (as opposed to "NOT
                  NEW").
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.recent_flag = 0";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.recent_flag <> 0";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'ON':

                /*
                  ON <date>
                  Messages whose internal date (disregarding time and timezone)
                  is within the specified date.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                $datetime_value = date_create_from_format('j-M-Y', $search_value);
                if (!$datetime_value) {
                    /*
                     * Malformed datetime
                     */
                    return FALSE;
                }

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.internal_date BETWEEN '{$datetime_value->format('Y-m-d')} 00:00:00' AND '{$datetime_value->format('Y-m-d')} 23:59:59'";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.internal_date < '{$datetime_value->format('Y-m-d')} 00:00:00' OR dbmail_physmessage.internal_date > '{$datetime_value->format('Y-m-d')} 23:59:59'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);
                break;

            case 'RECENT':

                /*
                  RECENT
                  Messages that have the \Recent flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.recent_flag = 1";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.recent_flag <> 1";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'SEEN':

                /*
                  SEEN
                  Messages that have the \Seen flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.seen_flag = 1";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.seen_flag <> 1";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'SENTBEFORE':

                /*
                  SENTBEFORE <date>
                  Messages whose [RFC-2822] Date: header (disregarding time and
                  timezone) is earlier than the specified date.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $header_name_id = $this->get_header_id_by_header_name('date');
                if (!$header_name_id) {
                    /*
                     * Missing lookup value - return empty string (this is NON an error!)
                     */
                    return array();
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                $datetime_value = date_create_from_format('j-M-Y', $search_value);
                if (!$datetime_value) {
                    /*
                     * Malformed datetime
                     */
                    return FALSE;
                }

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_header_alias = "dbmail_header_{$sequence}";

                $sequence++;
                $dbmail_headervalue_alias = "dbmail_headervalue_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_header AS {$dbmail_header_alias} ON dbmail_physmessage.id = {$dbmail_header_alias}.physmessage_id",
                    "INNER JOIN dbmail_headervalue AS {$dbmail_headervalue_alias} ON {$dbmail_header_alias}.headervalue_id = {$dbmail_headervalue_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.datefield < '{$datetime_value->format('Y-m-d')} 00:00:00'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.datefield >= '{$datetime_value->format('Y-m-d')} 00:00:00'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'SENTON':

                /*
                  SENTON <date>
                  Messages whose [RFC-2822] Date: header (disregarding time and
                  timezone) is within the specified date.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $header_name_id = $this->get_header_id_by_header_name('date');
                if (!$header_name_id) {
                    /*
                     * Missing lookup value - return empty string (this is NON an error!)
                     */
                    return array();
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                $datetime_value = date_create_from_format('j-M-Y', $search_value);
                if (!$datetime_value) {
                    /*
                     * Malformed datetime
                     */
                    return FALSE;
                }

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_header_alias = "dbmail_header_{$sequence}";

                $sequence++;
                $dbmail_headervalue_alias = "dbmail_headervalue_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_header AS {$dbmail_header_alias} ON dbmail_physmessage.id = {$dbmail_header_alias}.physmessage_id",
                    "INNER JOIN dbmail_headervalue AS {$dbmail_headervalue_alias} ON {$dbmail_header_alias}.headervalue_id = {$dbmail_headervalue_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.datefield = '{$datetime_value->format('Y-m-d')} 00:00:00'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.datefield <> '{$datetime_value->format('Y-m-d')} 00:00:00'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);
                break;
            case 'SENTSINCE':

                /*
                  SENTSINCE <date>
                  Messages whose [RFC-2822] Date: header (disregarding time and
                  timezone) is within or later than the specified date.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $header_name_id = $this->get_header_id_by_header_name('date');
                if (!$header_name_id) {
                    /*
                     * Missing lookup value - return empty string (this is NON an error!)
                     */
                    return array();
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                $datetime_value = date_create_from_format('j-M-Y', $search_value);
                if (!$datetime_value) {
                    /*
                     * Malformed datetime
                     */
                    return FALSE;
                }

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_header_alias = "dbmail_header_{$sequence}";

                $sequence++;
                $dbmail_headervalue_alias = "dbmail_headervalue_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_header AS {$dbmail_header_alias} ON dbmail_physmessage.id = {$dbmail_header_alias}.physmessage_id",
                    "INNER JOIN dbmail_headervalue AS {$dbmail_headervalue_alias} ON {$dbmail_header_alias}.headervalue_id = {$dbmail_headervalue_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.datefield >= '{$datetime_value->format('Y-m-d')} 00:00:00'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.datefield < '{$datetime_value->format('Y-m-d')} 00:00:00'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);
                break;
            case 'SINCE':

                /*
                  SINCE <date>
                  Messages whose internal date (disregarding time and timezone)
                  is within or later than the specified date.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                $datetime_value = date_create_from_format('j-M-Y', $search_value);
                if (!$datetime_value) {
                    /*
                     * Malformed datetime
                     */
                    return FALSE;
                }

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.internal_date >= '{$datetime_value->format('Y-m-d')} 00:00:00'";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.internal_date < '{$datetime_value->format('Y-m-d')} 00:00:00'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'SMALLER':

                /*
                  SMALLER <n>
                  Messages with an [RFC-2822] size smaller than the specified
                  number of octets.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.rfcsize < '{$this->dbmail->escape($search_value)}'";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_physmessage.rfcsize >= '{$this->dbmail->escape($search_value)}'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'SUBJECT':

                /*
                  SUBJECT <string>
                  Messages that contain the specified string in the envelope
                  structure's SUBJECT field.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $header_name_id = $this->get_header_id_by_header_name('subject');
                if (!$header_name_id) {
                    /*
                     * Missing lookup value - return empty string (this is NON an error!)
                     */
                    return array();
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_header_alias = "dbmail_header_{$sequence}";

                $sequence++;
                $dbmail_headervalue_alias = "dbmail_headervalue_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_header AS {$dbmail_header_alias} ON dbmail_physmessage.id = {$dbmail_header_alias}.physmessage_id",
                    "INNER JOIN dbmail_headervalue AS {$dbmail_headervalue_alias} ON {$dbmail_header_alias}.headervalue_id = {$dbmail_headervalue_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue LIKE '%{$this->dbmail->escape($search_value)}%'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue NOT LIKE '%{$this->dbmail->escape($search_value)}%'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);
                break;
            case 'TEXT':

                /*
                  TEXT <string>
                  Messages that contain the specified string in the header or
                  body of the message.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_partlists_alias = "dbmail_partlists_{$sequence}";

                $sequence++;
                $dbmail_mimeparts_alias = "dbmail_mimeparts_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_partlists AS {$dbmail_partlists_alias} ON dbmail_physmessage.id = {$dbmail_partlists_alias}.physmessage_id",
                    "INNER JOIN dbmail_mimeparts AS {$dbmail_mimeparts_alias} ON {$dbmail_partlists_alias}.part_id = {$dbmail_mimeparts_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_mimeparts_alias}.data LIKE '%{$this->dbmail->escape($search_value)}%'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_mimeparts_alias}.data NOT LIKE '%{$this->dbmail->escape($search_value)}%'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'TO':

                /*
                  TO <string>
                  Messages that contain the specified string in the envelope
                  structure's TO field.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $header_name_id = $this->get_header_id_by_header_name('to');
                if (!$header_name_id) {
                    /*
                     * Missing lookup value - return empty string (this is NON an error!)
                     */
                    return array();
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Physmessages table needed!!!!
                 */
                $search_conditions->needs_physmessages = TRUE;

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_header_alias = "dbmail_header_{$sequence}";

                $sequence++;
                $dbmail_headervalue_alias = "dbmail_headervalue_{$sequence}";

                $search_conditions->additional_joins = array(
                    "INNER JOIN dbmail_header AS {$dbmail_header_alias} ON dbmail_physmessage.id = {$dbmail_header_alias}.physmessage_id",
                    "INNER JOIN dbmail_headervalue AS {$dbmail_headervalue_alias} ON {$dbmail_header_alias}.headervalue_id = {$dbmail_headervalue_alias}.id"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue LIKE '%{$this->dbmail->escape($search_value)}%'";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_header_alias}.headername_id = {$header_name_id} AND {$dbmail_headervalue_alias}.headervalue NOT LIKE '%{$this->dbmail->escape($search_value)}%'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'UID':

                /*
                  UID <sequence set>
                  Messages with unique identifiers corresponding to the specified
                  unique identifier set.  Sequence set ranges are permitted.
                 */
                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.unique_id = '{$this->dbmail->escape($search_value)}'";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.unique_id <> '{$this->dbmail->escape($search_value)}'";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'UNANSWERED':

                /*
                  UNANSWERED
                  Messages that do not have the \Answered flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.answered_flag = 0";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.answered_flag <> 0";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'UNDELETED':

                /*
                  UNDELETED
                  Messages that do not have the \Deleted flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.deleted_flag = 0";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.deleted_flag <> 0";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'UNDRAFT':

                /*
                  UNDRAFT
                  Messages that do not have the \Draft flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.draft_flag = 0";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.draft_flag <> 0";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'UNFLAGGED':

                /*
                  UNFLAGGED
                  Messages that do not have the \Flagged flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.flagged_flag = 0";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.flagged_flag <> 0";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            case 'UNKEYWORD':

                /*
                  UNKEYWORD <flag>
                  Messages that do not have the specified keyword flag set.
                 */

                if (!array_key_exists(1, $items)) {
                    /*
                     * Error - Missing parameters
                     */
                    return FALSE;
                }

                $search_value = $this->_cleanup_search_parameter($items[1]);

                /*
                 * Set tables alias
                 */
                $sequence++;
                $dbmail_keywords_alias = "dbmail_keywords{$sequence}";

                $search_conditions->additional_joins = array(
                    "LEFT JOIN dbmail_keywords AS {$dbmail_keywords_alias} on dbmail_messages.message_idnr = {$dbmail_keywords_alias}.message_idnr AND {$dbmail_keywords_alias}.keyword = '{$this->dbmail->escape($search_value)}'"
                );

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "{$dbmail_keywords_alias}.message_idnr IS NULL";
                } else {
                    $search_conditions->additional_where_conditions = "{$dbmail_keywords_alias}.message_idnr IS NOT NULL";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 2);

                break;
            case 'UNSEEN':

                /*
                  UNSEEN
                  Messages that do not have the \Seen flag set.
                 */

                $search_conditions->additional_joins = array();

                if (!$is_not) {
                    $search_conditions->additional_where_conditions = "dbmail_messages.seen_flag = 0";
                } else {
                    $search_conditions->additional_where_conditions = "dbmail_messages.seen_flag <> 0";
                }

                /*
                 * Unset managed items
                 */
                $items = array_slice($items, 1);

                break;
            default:

                /*
                 * Invalid operand!
                 */
                // $this->rc->console('_apply_search_parameters - Invalid operand!');

                return FALSE;
        }

        /*
         * Reorder items array
         */
        $items = array_values($items);


        return $search_conditions;
    }

    /**
     * Format message-id header according to 'dbmail_referencesfield' content
     */
    private function clean_up_thread_message_id($message_id_header = '') {

        /**
         * Remove trailing / leading angle brackets (according to 'dbmail_referencesfield' content)
         */
        return rtrim(ltrim($message_id_header, '<'), '>');
    }

    /**
     * Retrieve base thread message-id and full ancesters list.
     * Try to use cache as much as we can!!!!!!
     * @param int $physmessage_id
     * @param int $message_idnr
     * @param string $folder
     * @return mixed
     */
    private function _get_thread_details($physmessage_id = NULL, $message_idnr = NULL, $folder = NULL) {

        /**
         * Set cache key
         */
        $rcmh_cached_key = "THREAD_DETAILS_MSG_" . $physmessage_id;

        /*
         * Do we already have a cache entry?
         */
        $rcmh_cached = $this->get_cache($rcmh_cached_key);
        if ($rcmh_cached &&
                isset($rcmh_cached->base_thread_message_id) &&
                strlen($rcmh_cached->base_thread_message_id) > 0 &&
                isset($rcmh_cached->ancesters) &&
                is_array($rcmh_cached->ancesters)) {
            return $rcmh_cached;
        }

        /**
         * Retrieve message ancesters
         * 
         * https://tools.ietf.org/html/rfc5256
         * 
         * If a message contains a References header line, then use the
         * Message IDs in the References header line as the references.
         * 
         * If a message does not contain a References header line, or
         * the References header line does not contain any valid
         * Message IDs, then use the first (if any) valid Message ID
         * found in the In-Reply-To header line as the only reference
         * (parent) for this message.
         * 
         *    Note: Although [RFC2822] permits multiple Message IDs in
         *    the In-Reply-To header, in actual practice this
         *    discipline has not been followed.  For example,
         *    In-Reply-To headers have been observed with message
         *    addresses after the Message ID, and there are no good
         *    heuristics for software to determine the difference.
         *    This is not a problem with the References header,
         *    however.
         * 
         * If a message does not contain an In-Reply-To header line, or
         * the In-Reply-To header line does not contain a valid Message
         * ID, then the message does not have any references (NIL).
         */
        $response = new stdClass();
        $response->base_thread_message_id = NULL;
        $response->ancesters = array();

        /**
         * First step, search within 'referencesfield' table (first entry is the base message!)
         */
        $references_sql = " SELECT referencesfield "
                . " FROM dbmail_referencesfield "
                . " WHERE physmessage_id = '{$this->dbmail->escape($physmessage_id)}' "
                . " ORDER BY id ASC ";

        $references_result = $this->dbmail->query($references_sql);
        while ($row = $this->dbmail->fetch_assoc($references_result)) {
            $response->ancesters[] = trim($row['referencesfield']);
        }

        /**
         * Done?
         */
        if (count($response->ancesters) > 0 && isset($response->ancesters[0]) && strlen($response->ancesters[0]) > 0) {
            $response->base_thread_message_id = $response->ancesters[0];
            $this->update_cache($rcmh_cached_key, $response);
            return $response;
        }

        /**
         * Second Step, sometimes dbmail 'referencesfield' is useless even if message 
         * headers contains 'references' entry, so let's retrieve message datails. 
         */
        $message = $this->retrieve_message($message_idnr, $folder, array(), FALSE);
        $raw_headers_references_string = isset($message->references) ? $message->references : '';
        foreach (explode(' ', $raw_headers_references_string) as $item) {
            if (strlen(trim($item)) > 0) {
                $response->ancesters[] = $this->clean_up_thread_message_id(trim($item));
            }
        }

        /**
         * Done?
         */
        if (count($response->ancesters) > 0 && isset($response->ancesters[0]) && strlen($response->ancesters[0]) > 0) {
            $response->base_thread_message_id = $response->ancesters[0];
            $this->update_cache($rcmh_cached_key, $response);
            return $response;
        }

        /**
         * Third Step, 'references' not found on dbmail 'referencesfield' table 
         * nor within message headers. Do we have a 'in-reply-to' header so we can consider it as ancester?
         */
        if (isset($message->in_reply_to) && strlen($message->in_reply_to) > 0) {
            $response->ancesters[] = $this->clean_up_thread_message_id(trim($message->in_reply_to));
        }

        /**
         * Done?
         */
        if (count($response->ancesters) > 0 && isset($response->ancesters[0]) && strlen($response->ancesters[0]) > 0) {
            $response->base_thread_message_id = $response->ancesters[0];
            $this->update_cache($rcmh_cached_key, $response);
            return $response;
        }

        /**
         * Nothing found? Than use current 'message-id' header as base thread (or 
         * generate a dummy one if not found) and leave an empty ancesters list.
         */
        $response->base_thread_message_id = isset($message->messageID) && strlen($message->messageID) > 0 ? $this->clean_up_thread_message_id(trim($message->messageID)) : microtime() . rand(0, 10000);
        $response->ancesters = array();
        $this->update_cache($rcmh_cached_key, $response);
        return $response;
    }

    /**
     * Retrieve a physmessages list wich references a specific message 
     * @param string 'message-id' header to search for
     * @return array
     */
    private function get_thread_related_message_idnrs($header_message_id = NULL) {

        $response = array();

        /**
         * Nothing to do....
         */
        if (strlen($header_message_id) == 0) {
            return $response;
        }

        /**
         * Retrieve subscribed folders
         */
        $target = $this->list_folders_subscribed('', '*', 'mail', null, true);
        $folders = $this->_format_folders_list($target);

        /*
         *  map mailboxes ID
         */
        $mailboxes = array();
        foreach ($folders as $folder_name) {

            $mail_box_idnr = $this->get_mail_box_id($folder_name);
            if (!$mail_box_idnr) {
                continue;
            }

            $mailboxes[] = $mail_box_idnr;
        }

        if (count($mailboxes) == 0) {
            return $response;
        }

        /**
         * remove leading / trailing square barackets from '$header_message_id' according to DBMail format 
         */
        $formatted_header_message_id = rtrim(ltrim($header_message_id, '<'), '>');

        /**
         * retrieve messages within subscribed folders
         */
        $sql = " select dbmail_messages.message_idnr, dbmail_messages.physmessage_id "
                . " from dbmail_referencesfield "
                . " inner join dbmail_messages on dbmail_referencesfield.physmessage_id = dbmail_messages.physmessage_id "
                . " where dbmail_referencesfield.referencesfield = '{$this->dbmail->escape($formatted_header_message_id)}' "
                . " and dbmail_messages.mailbox_idnr in (" . implode(',', $mailboxes) . ") ";

        $result = $this->dbmail->query($sql);

        while ($msg = $this->dbmail->fetch_assoc($result)) {
            if (!array_key_exists($msg['message_idnr'], $response)) {
                $response[$msg['message_idnr']] = $msg['physmessage_id'];
            }
        }

        return $response;
    }

    /**
     * This will build a flattern array containing threaded messages list
     * @param array messages list
     * @param int parent message id
     * @param array referente to response array
     */
    private function _flattern_threaded_messages($messages = array(), $parent_uid = NULL, &$response = array()) {

        foreach ($messages as $message) {

            if (strlen($parent_uid) == 0 && strlen($message->parent_uid) == 0) {
                /*
                 * OK
                 */
            } else if ($parent_uid == $message->parent_uid) {
                /*
                 * OK
                 */
            } else {
                continue;
            }

            $response[] = $message;

            $this->_flattern_threaded_messages($messages, $message->uid, $response);
        }
    }

    /**
     * 'usort' callback method to sort by 'internal-date' property a 
     * threaded messages list.
     */
    private function _sort_threaded_messages_list() {

        return function ($a, $b) {

            if (!isset($a->depth) || !isset($a->depth) || $a->depth == 0 || $b->depth == 0) {
                /**
                 * Do not change root items!
                 */
                return 0;
            }

            return strnatcmp($a->internaldate_GMT, $b->internaldate_GMT);
        };
    }

}
