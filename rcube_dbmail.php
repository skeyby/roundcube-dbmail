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
 * !!! IMPORTANT !!!
 * Use the official PEAR Mail_mimeDecode library, changing following line in 'composer.json'
 * change  "pear/mail_mime-decode": ">=1.5.5",
 * to      "pear-pear.php.net/Mail_mimeDecode": ">=1.5.5",
 * 
 * ----------------------------
 * 
 * Notes:
 * 
 * 1. DBMAIL nightly cleanup every cached data (envelope / headers) for deleted
 *    messages, so we don't need to manually delete those records
 */
class rcube_dbmail extends rcube_storage {

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
    private $headers_lookup = array();
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
        // 'THREAD' => array('ORDEREDSUBJECT'), # Temporaly removed since the code doesn't support it
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

        // set user_idnr (if found)
        if (is_null($this->user_idnr) && strlen($_SESSION['user_idnr']) > 0) {
            $this->user_idnr = $_SESSION['user_idnr'];
        }

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
        $query = "SELECT user_idnr, passwd, encryption_type "
                . " FROM dbmail_users "
                . " WHERE userid = '{$this->dbmail->escape($user)}' ";

        $res = $this->dbmail->query($query);

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

        // valid user? store user identity within session data
        if ($valid_user) {
            $this->user_idnr = $row['user_idnr'];
            $_SESSION['user_idnr'] = $this->user_idnr;
        }

        return $valid_user;
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
     * Get messages count for a specific folder.
     *
     * @param  string  $folder  Folder name
     * @param  string  $mode    Mode for count [ALL|THREADS|UNSEEN|RECENT|EXISTS]
     * @param  boolean $force   Force reading from server and update cache
     * @param  boolean $status  Enables storing folder status info (max UID/count),
     *                          required for folder_status()
     *
     * @return int     Number of messages
     */
    public function count($folder = null, $mode = 'ALL', $force = false, $status = true) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        /**
         *  mailbox exists?
         */
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        /**
         *  ACLs check ('lookup' and 'read' grants required )
         */
        $ACLs = $this->_get_acl(NULL, $mailbox_idnr);
        if (!is_array($ACLs) || !in_array(self::ACL_LOOKUP_FLAG, $ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
            /**
             *  Unauthorized!
             */
            return FALSE;
        }

        /*
         *  init search conditions
         */
        $search_conditions = NULL;
        if (is_array($this->search_set) && array_key_exists(0, $this->search_set)) {
            $search_conditions = $this->format_search_parameters($this->search_set[0]);
        }

        /*
         *  set additional join tables according to supplied search / filter conditions
         */
        $additional_joins = "";
        if (is_object($search_conditions) && property_exists($search_conditions, 'additional_join_tables')) {
            $additional_joins .= " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id ";
            $additional_joins .= " {$search_conditions->additional_join_tables} ";
        }

        /*
         * Set base 'where' conditions
         */
        $where_conditions = " WHERE dbmail_messages.mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} "
                . " AND dbmail_messages.status < " . self::MESSAGE_STATUS_DELETE;

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
         * Set 'distinct' clause when additional joins are needed
         */
        $distinct_clause = (strlen($additional_joins) > 0 ? 'DISTINCT' : '');

        /*
         * Prepare base query
         */
        $query = " SELECT COUNT({$distinct_clause} dbmail_messages.message_idnr) AS items_count "
                . " FROM dbmail_messages ";
        $query .= " {$additional_joins} ";
        $query .= " {$where_conditions} ";

        /*
         * Before executing query (and if '$force' == FALSE), try to get a temporary content (if exists)
         */
        $temp_key = "METHOD_COUNT_" . md5($query);

        $temp_contents = $this->get_temp_value($temp_key);
        if (!$force && $temp_contents) {
            return $temp_contents;
        }

        /*
         * Execute query
         */
        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        $items_count = ($row['items_count'] > 0 ? $row['items_count'] : 0);

        /*
         * Save query output within temporary contents
         */
        $this->set_temp_value($temp_key, $items_count);

        /*
         *  Cache messages count and latest message id
         */
        if ($mode == 'ALL' && $status) {
            $this->set_folder_stats($folder, 'cnt', $items_count);
            $this->set_folder_stats($folder, 'maxuid', ($items_count ? $this->get_latest_message_idnr($folder) : 0));
        }

        return $items_count;
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

        if (!is_array($uids) || count($uids) == 0) {
            /*
             * Empry set supplied!
             */
            return array();
        }

        foreach ($uids as &$uid) {
            /*
             *  escape arguments
             */
            $uid = $this->dbmail->escape($uid);
        }

        $query = " SELECT seen_flag, answered_flag, deleted_flag, flagged_flag, recent_flag, draft_flag "
                . " FROM dbmail_messages "
                . " WHERE message_idnr in (" . implode(',', $uids) . ") "
                . " AND mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

        $res = $this->dbmail->query($query);

        while ($row = $this->dbmail->fetch_assoc($res)) {

            $result[$uid] = array(
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
     * @param   string   $folder     Folder name
     * @param   int      $page       Current page to list
     * @param   string   $sort_field Header field to sort by
     * @param   string   $sort_order Sort order [ASC|DESC]
     * @param   int      $slice      Number of slice items to extract from result array
     *
     * @return  array    Indexed array with message header objects
     */
    public function list_messages($folder = null, $page = null, $sort_field = null, $sort_order = null, $slice = 0) {

        $search_conditions = NULL;
        if (is_array($this->search_set) && array_key_exists(0, $this->search_set)) {
            $search_conditions = $this->format_search_parameters($this->search_set[0]);
        }

        return $this->_list_messages($folder, $page, $sort_field, $sort_order, $slice, $search_conditions, NULL);
    }

    /**
     * Return sorted list of message UIDs
     *
     * @param string $folder     Folder to get index from
     * @param string $sort_field Sort column
     * @param string $sort_order Sort order [ASC, DESC]
     *
     * @return rcube_result_index|rcube_result_thread List of messages (UIDs)
     */
    public function index($folder = null, $sort_field = null, $sort_order = null) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        // ACLs check ('lookup' and 'read' grants required )
        $ACLs = $this->_get_acl($folder);
        if (!is_array($ACLs) || !in_array(self::ACL_LOOKUP_FLAG, $ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
            // Unauthorized!
            return FALSE;
        }

        // get messages list
        $result_index_str = "";
        $messages = $this->_list_messages($folder, 0, $sort_field, $sort_order);
        foreach ($messages as $message) {
            $result_index_str .= " {$message->uid}";
        }

        $index = new rcube_result_index($folder, "* SORT {$result_index_str}");

        return $index;
    }

    /**
     * Invoke search request to the server.
     *
     * @param  string  $folder     Folder name to search in
     * @param  string  $str        Search criteria
     * @param  string  $charset    Search charset
     * @param  string  $sort_field Header field to sort by
     *
     * @todo: Search criteria should be provided in non-IMAP format, eg. array
     */
    public function search($folder = null, $str = 'ALL', $charset = null, $sort_field = null) {

        // normalize target folder/s
        if (is_array($folder) && count($folder) > 0) {
            $folders = $folder;
        } elseif (strlen($folder) == 0) {
            $folders = array($folder);
        } else {
            $folders = array($this->folder);
        }

        // extract folders id
        $mail_box_idnr_list = array();
        foreach ($folders as $folder_name) {

            // Retrieve mailbox ID
            $mail_box_idnr = $this->get_mail_box_id($folder_name);
            if (!$mail_box_idnr) {
                // Not found - Skip!
                continue;
            }

            // ACLs check ('lookup' and 'read' grants required )
            $ACLs = $this->_get_acl(NULL, $mail_box_idnr);
            if (!is_array($ACLs) || !in_array(self::ACL_LOOKUP_FLAG, $ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
                // Unauthorized - Skip!
                continue;
            }

            // Add mailbox ID to mailboxes list
            $mail_box_idnr_list[] = $mail_box_idnr;
        }

        // format search conditions
        $search_conditions = $this->format_search_parameters($str);

        // get messages list
        $result_index_str = "";
        $messages = $this->_list_messages($folder, 0, $sort_field, 'ASC', 0, $search_conditions);
        foreach ($messages as $message) {
            $result_index_str .= " {$message->uid}";
        }

        $index = new rcube_result_index($folder, "* SORT {$result_index_str}");

        $this->search_set = array(
            $str,
            $index
        );

        return $index;
    }

    /**
     * Direct (real and simple) search request (without result sorting and caching).
     *
     * @param  string  $folder  Folder name to search in
     * @param  string  $str     Search string
     *
     * @return rcube_result_index  Search result (UIDs)
     */
    public function search_once($folder = null, $str = 'ALL') {

        // normalize target folder/s
        if (is_array($folder) && count($folder) > 0) {
            $folders = $folder;
        } elseif (strlen($folder) == 0) {
            $folders = array($folder);
        } else {
            $folders = array($this->folder);
        }

        // extract folders id
        $mail_box_idnr_list = array();
        foreach ($folders as $folder_name) {

            // Retrieve mailbox ID
            $mail_box_idnr = $this->get_mail_box_id($folder_name);
            if (!$mail_box_idnr) {
                // Not found - Skip!
                continue;
            }

            // ACLs check ('lookup' and 'read' grants required )
            $ACLs = $this->_get_acl(NULL, $mail_box_idnr);
            if (!is_array($ACLs) || !in_array(self::ACL_LOOKUP_FLAG, $ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
                // Unauthorized - Skip!
                continue;
            }

            // Add mailbox ID to mailboxes list
            $mail_box_idnr_list[] = $mail_box_idnr;
        }

        // format search conditions
        $search_conditions = $this->format_search_parameters($str);

        // get messages list
        $result_index_str = "";
        $messages = $this->_list_messages($folder, 0, NULL, 'ASC', 0, $search_conditions);
        foreach ($messages as $message) {
            $result_index_str .= " {$message->uid}";
        }

        $index = new rcube_result_index($folder, "* SORT {$result_index_str}");

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

        return $this->retrieve_message($uid);
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

        return $this->retrieve_message($uid);
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
     * @param int      $uid Message UID
     * @param resource $fp  File pointer to save the message
     *
     * @return string Message source string
     */
    public function get_raw_body($uid, $fp = null) {

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
        $mime = $this->fetch_part_lists($message_metadata['physmessage_id']);

        return $mime->body;
    }

    /**
     * Returns the message headers as string
     *
     * @param int $uid  Message UID
     *
     * @return string Message headers string
     */
    public function get_raw_headers($uid) {

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
        $mime = $this->fetch_part_lists($message_metadata['physmessage_id']);

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
     * @param string  $flag       Flag to set: UNDELETED, DELETED, SEEN, UNSEEN, FLAGGED, UNFLAGGED
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

        // validate target flag
        $flag_field = '';
        $flag_value = '';
        $required_ACL = '';
        switch ($flag) {
            case 'UNDELETED':
                $flag_field = 'deleted_flag';
                $flag_value = 0;
                $required_ACL = self::ACL_DELETED_FLAG;
                break;
            case 'DELETED':
                $flag_field = 'deleted_flag';
                $flag_value = 1;
                $required_ACL = self::ACL_DELETED_FLAG;
                break;
            case 'SEEN':
                $flag_field = 'seen_flag';
                $flag_value = 1;
                $required_ACL = self::ACL_SEEN_FLAG;
                break;
            case 'UNSEEN':
                $flag_field = 'seen_flag';
                $flag_value = 0;
                $required_ACL = self::ACL_SEEN_FLAG;
                break;
            case 'FLAGGED':
                $flag_field = 'flagged_flag';
                $flag_value = 1;
                $required_ACL = self::ACL_WRITE_FLAG;
                break;
            case 'UNFLAGGED':
                $flag_field = 'flagged_flag';
                $flag_value = 0;
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
            $query = "UPDATE dbmail_messages "
                    . "SET {$this->dbmail->escape($flag_field)} = {$this->dbmail->escape($flag_value)} "
                    . "WHERE message_idnr = {$this->dbmail->escape($message_uid)} ";

            if (!$this->dbmail->query($query)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
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

        $folder_cached = $this->get_cache("FOLDER_" . $folder);
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

        $this->update_cache("FOLDER_" . $folder, $options);

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

        /* This is creative hack to show both configurable infos for dbmail */
        $result['all']["Messages"]["storage"]["used"] = $used;
        $result['all']["Messages"]["storage"]["total"] = $total;
        $result['all']["Rules"]["storage"]["used"] = $sieveused;
        $result['all']["Rules"]["storage"]["total"] = $sievetotal;

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
     *
     * @return array Metadata entry-value hash array on success, NULL on error
     */
    public function get_metadata($folder, $entries, $options = array()) {
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
     * @return string header value on success, False on error
     */
    protected function get_header_value($header, $token) {

        ## Remove any trailing WSP
        $header = trim($header);

        ## Unfolding according to RFC 2822, chapter 2.2.3
        $header = str_replace("\r\n ", " ", $header);
        $header = str_replace("\r\n\t", " ", $header);
        ## Unfolding with compatibility with some non-standard mailers
        ## that only add \n instead of \r\n
        $header = str_replace("\n ", " ", $header);
        $header = str_replace("\n\t", " ", $header);

        // explode header by new line sign
        $rows = explode("\n", $header);

        // standard delimiter is ':', match '=' only for following properties
        $delimiter = $this->get_header_delimiter($token);

        // convert token to uppercase to perform case-insensitive search
        $ci_token = strtoupper($token);

        // loop each row searching for supplied token
        foreach ($rows as &$row) {

            // trim whitespaces
            $row = trim($row);

            // split row by ';' to manage multiple key=>value pairs within same row
            $items = explode(';', $row);

            foreach ($items as &$item) {

                $item = trim($item);

                if ($ci_token == substr(strtoupper($item), 0, strlen($ci_token))) {

                    // ok - string begins with '$token'
                    list($key, $value) = explode($delimiter, $item, 2);

                    // remove trailing / leading spaces from $value
                    $value = trim($value);

                    // when the header is composed, we
                    // remove trailing / leading quotes from $value
                    // remove trailing / leading double quotes from $value
                    if ($delimiter == "=") {
                        $value = trim($value, "'");
                        $value = trim($value, "\"");
                    }

                    return $value;
                }
            }
        }

        return FALSE;
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
     * @param array $message_data 
     * @param boolean $getBody whenever to retrieve body content too (instead of headers only)
     * @return mixed
     */
    private function retrieve_message($message_idnr, $message_data = FALSE, $getBody = TRUE) {

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
            if (is_array($message_data)) {

                $rcmh_cached->folder = $message_data['folder_record']['name'];
                $rcmh_cached->flags["SEEN"] = ($message_data['seen_flag'] == 1 ? TRUE : FALSE);
                $rcmh_cached->flags["ANSWERED"] = ($message_data['answered_flag'] == 1 ? TRUE : FALSE);
                $rcmh_cached->flags["DELETED"] = ($message_data['deleted_flag'] == 1 ? TRUE : FALSE);
                $rcmh_cached->flags["FLAGGED"] = ($message_data['flagged_flag'] == 1 ? TRUE : FALSE);
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
             * Get folder properties
             */
            $folder_record = $this->get_folder_record($message_record['mailbox_idnr']);
            if (!$folder_record) {
                // Not found!
                return FALSE;
            }

            /*
             * Ok - prepare message data array
             */
            $message_data = array(
                'message_idnr' => $message_record["message_idnr"],
                'physmessage_id' => $message_record['physmessage_id'],
                'message_size' => $message_record["messagesize"],
                'seen_flag' => $message_record["seen_flag"],
                'answered_flag' => $message_record["answered_flag"],
                'deleted_flag' => $message_record["deleted_flag"],
                'flagged_flag' => $message_record["flagged_flag"],
                'folder_record' => array(
                    'name' => $folder_record['name']
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

        if ($getBody) {

            $mime_decoded = $this->decode_raw_message($mime->header . $mime->body);
            if (!$mime_decoded) {
                return FALSE;
            }

            $rcmh->structure = $this->get_structure($mime_decoded);
        }

        // update cached contents
        $this->update_cache($rcmh_cached_key, $rcmh);

        return $rcmh;
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
     * @param   array    $search_conditions Search conditions
     *
     * @return  array    Indexed array with message header objects
     */
    private function _list_messages($folder = null, $page = null, $sort_field = null, $sort_order = null, $slice = 0, $search_conditions = NULL) {

        if (strlen($folder) == 0) {
            $folder = $this->folder;
        }

        /*
         * Get current mailbox folder ID
         */
        $mailbox_idnr = $this->get_mail_box_id($folder);

        /*
         * ACLs check ('lookup' and 'read' grants required )
         */
        $ACLs = $this->_get_acl(NULL, $mailbox_idnr);
        if (!is_array($ACLs) || !in_array(self::ACL_LOOKUP_FLAG, $ACLs) || !in_array(self::ACL_READ_FLAG, $ACLs)) {
            /*
             *  Unauthorized!
             */
            return FALSE;
        }

        /*
         * Validate sort order (use default when not supplied)
         */
        $sort_order = (strtoupper($sort_order) == 'DESC' ? 'DESC' : 'ASC');

        /*
         * Set query offset / limit
         */
        $page = ((int) $page > 0 ? $page : $this->list_page);
        $query_offset = ($page > 0 ? (($page - 1) * $this->page_size) : 0);
        $query_limit = $this->page_size;

        /*
         * Set additional join tables according to supplied search conditions
         */
        $additional_joins = "";
        if (is_object($search_conditions) && property_exists($search_conditions, 'additional_join_tables')) {
            $additional_joins .= " {$search_conditions->additional_join_tables}";
        }

        /*
         * "Base Condition" is that the message should not be EXPUNGED (thus DELETED)
         */
        $where_conditions = " WHERE dbmail_messages.status < " . self::MESSAGE_STATUS_DELETE;

        /*
         * Set where conditions according to supplied search / filter conditions
         */
        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_filter_str') && strlen($search_conditions->formatted_filter_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_filter_str} )";
        }

        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_search_str') && strlen($search_conditions->formatted_search_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_search_str} )";
        }

        /*
         * Do we want deleted messages?
         */
        if ($this->options["skip_deleted"]) {
            $where_conditions .= " AND dbmail_messages.deleted_flag = 0 ";
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

                $additional_joins .= " "
                        . " LEFT JOIN dbmail_header AS sort_dbmail_header ON dbmail_physmessage.id = sort_dbmail_header.physmessage_id AND sort_dbmail_header.headername_id = {$this->dbmail->escape($header_id)} "
                        . " LEFT JOIN dbmail_headervalue AS sort_dbmail_headervalue ON sort_dbmail_header.headervalue_id = sort_dbmail_headervalue.id ";

                $sort_condition = " ORDER BY sort_dbmail_headervalue.sortfield {$this->dbmail->escape($sort_order)} ";
                break;
            case 'size':
                /*
                 *  'size' value is stored into 'dbmail_physmessage' table - no additional joins needed
                 */
                $sort_condition = " ORDER BY dbmail_physmessage.messagesize {$this->dbmail->escape($sort_order)} ";
                break;
            default:
                /*
                 *  natural sort - do nothing!
                 */
                $sort_condition = " ORDER BY dbmail_messages.message_idnr {$this->dbmail->escape($sort_order)} ";
                break;
        }

        /*
         * Set 'limit' clause 
         */
        $limit_condition = " LIMIT {$this->dbmail->escape($query_offset)}, {$this->dbmail->escape($query_limit)} ";

        /*
         * When no additional joins needed, avoid 'DISTINCT' clause
         */
        $distinct_clause = (strlen($additional_joins) > 0 ? 'DISTINCT' : '');

        /*
         *  Prepare base query
         */
        $query = " SELECT $distinct_clause dbmail_messages.message_idnr, dbmail_messages.physmessage_id, "
                . " dbmail_physmessage.messagesize, dbmail_messages.seen_flag, "
                . " dbmail_messages.answered_flag, dbmail_messages.deleted_flag, "
                . " dbmail_messages.flagged_flag "
                . " FROM dbmail_messages "
                . " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id AND dbmail_messages.mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

        $query .= " {$additional_joins} ";
        $query .= " {$where_conditions} ";
        $query .= " {$sort_condition} ";
        $query .= " {$limit_condition} ";

        $res = $this->dbmail->query($query);

        $headers = array();
        $msg_index = $query_offset++;
        while ($msg = $this->dbmail->fetch_assoc($res)) {

            $message_data = array(
                'message_idnr' => $msg["message_idnr"],
                'physmessage_id' => $msg['physmessage_id'],
                'message_size' => $msg["messagesize"],
                'seen_flag' => $msg["seen_flag"],
                'answered_flag' => $msg["answered_flag"],
                'deleted_flag' => $msg["deleted_flag"],
                'flagged_flag' => $msg["flagged_flag"],
                'folder_record' => array(
                    'name' => $folder
                ),
                'mailbox_idnr' => $mailbox_idnr
            );

            $headers[$msg_index] = $this->retrieve_message($msg["message_idnr"], $message_data, FALSE);

            $msg_index++;
        }

        if ($slice) {
            $headers = array_slice($headers, -$slice, $slice);
        }

        return array_values($headers);
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
     * @return stdClass
     */
    private function fetch_part_lists($physmessage_id) {

        /*
         * Cached content exists?
         */
//        $cache_key = "RAW_MESSAGE_{$physmessage_id}";
//        $raw_message_cached = $this->get_cache($cache_key);
//        if (is_object($raw_message_cached)) {
//            return $raw_message_cached;
//        }

        $query = " SELECT dbmail_partlists.part_depth, dbmail_partlists.is_header, dbmail_mimeparts.data "
                . " FROM dbmail_partlists "
                . " INNER JOIN dbmail_mimeparts on dbmail_partlists.part_id = dbmail_mimeparts.id "
                . " WHERE dbmail_partlists.physmessage_id = {$this->dbmail->escape($physmessage_id)} "
                . " ORDER BY dbmail_partlists.part_key, dbmail_partlists.part_order ASC ";

        $result = $this->dbmail->query($query);

        $mimeParts = [];
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
        $blist = [];
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
            if ($is_header && preg_match('~^content-type:\s+.*;(\r?\n\s.*)*\s+boundary="?([a-z0-9\'()+_,-./:=\?]*)~mi', $blob, $matches)) {
                list(,, $boundary) = $matches;
                $got_boundary = true;
                $blist[$depth] = $boundary;
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
            console("Part Insert, physmessage id: " . $physmessage_id);
            console("Part Insert, is header:      " . $is_header);
            console("Part Insert, part key:       " . $part_key);
            console("Part Insert, part depth:     " . $part_depth);
            console("Part Insert, part order:     " . $part_order);
            console("Part Insert, hash:           " . $hash);
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
            console("Store mime object");
            console($mime_decoded);
            console("Store mime object - part key:   " . $part_key);
            console("Store mime object - part depth: " . $part_depth);
            console("Store mime object - part order: " . $part_order);
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
            //Console("We have a message");
            if (!$this->_part_insert($physmessage_id, $mime_decoded->body, 0, $part_key, $part_depth, $part_order)) {
                return FALSE;
            }
        } elseif ($part_depth == 0) {
            //Console("Empty body for first level");
            if (!$this->_part_insert($physmessage_id, "This is a multi-part message in MIME format.", 0, $part_key, $part_depth, $part_order)) {
                return FALSE;
            }
        }

        /*
         *  Do we have additional parts?
         */
        if (property_exists($mime_decoded, 'parts') && is_array($mime_decoded->parts)) {
            //Console("We have parts");

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
     * @param int $expiresAt
     */
    private function set_temp_value($key = '', $content = '', $expiresAt = NULL) {

        if (strlen($expiresAt) == 0) {
            /*
             * Set default TTL if none supplied
             */
            $expiresAt = time() + self::TEMP_TTL;
        }

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
     */
    private function unset_temp_value($key = '') {

        if (is_array($_SESSION) && array_key_exists($key, $_SESSION)) {
            unset($_SESSION[$key]);
        }

        return TRUE;
    }

}
