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
 *    $config['dbmail_dsn'] = 'mysql://user:pass@host/db'; # dsn connection string
 *    $config['dbmail_hash'] = 'sha1'; # hashing method to use, must coincide with dbmail.conf - sha1, md5, sha256, sha512, whirlpool. sha1 is the default
 *    $config['dbmail_fixed_headername_cache'] = FALSE; # add new headernames (if not exists) in 'dbmail_headername' when saving messages
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

    private $user_idnr = null;
    private $namespace = null;
    private $delimiter = null;
    private $dbmail = null;
    private $rcubeInstance = null;
    private $err_no = 0;
    private $err_str = '';
    private $response_code = null;
    private $searchable_headers = array(
        'x-priority',
        'subject',
        'from',
        'to',
        'cc',
        'bcc'
    );
    private $imap_capabilities = array(
        'ACL',
        'ANNOTATE-EXPERIMENT-1',
        'AUTH=',
        'BINARY',
        'CATENATE',
        'CHILDREN',
        'COMPRESS=DEFLATE',
        'CONDSTORE',
        'CONTEXT=SEARCH',
        'CONTEXT=SORT',
        'CONVERT',
        'CREATE-SPECIAL-USE',
        'ENABLE',
        'ESEARCH',
        'ESORT',
        'FILTERS',
        'I18NLEVEL=1',
        'I18NLEVEL=2',
        'ID',
        'IDLE',
        'IMAPSIEVE=',
        'LANGUAGE',
        'LIST-EXTENDED',
        'LIST-STATUS',
        'LITERAL+',
        'LOGIN-REFERRALS',
        'LOGINDISABLED',
        'MAILBOX-REFERRALS',
        'METADATA',
        'METADATA-SERVER',
        'MOVE',
        'MULTIAPPEND',
        'MULTISEARCH',
        'NAMESPACE',
        'NOTIFY',
        'QRESYNC',
        'QUOTA',
        'RIGHTS=',
        'SASL-IR',
        'SEARCH=FUZZY',
        'SEARCHRES',
        'SORT',
        'SORT=DISPLAY',
        'SPECIAL-USE',
        'STARTTLS',
        'THREAD',
        'UIDPLUS',
        'UNSELECT',
        'URLFETCH=BINARY',
        'URL-PARTIAL',
        'URLAUTH',
        'UTF8=ACCEPT',
        'UTF8=ALL',
        'UTF8=APPEND',
        'UTF8=ONLY',
        'UTF8=USER',
        'WITHIN'
    );

    const MESSAGE_STATUS_NEW = 0;
    const MESSAGE_STATUS_SEEN = 1;
    const MESSAGE_STATUS_DELETE = 2;
    const MESSAGE_STATUS_PURGE = 3;
    const MESSAGE_STATUS_UNUSED = 4;
    const MESSAGE_STATUS_INSERT = 5;
    const MESSAGE_STATUS_ERROR = 6;

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
     * Connect to the server
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
        // TO DO!!!!!!
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

        //return $this->search_set;

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
        $sess_key = "STORAGE_$cap";

        if (!isset($_SESSION[$sess_key])) {
            if (!$this->check_connection()) {
                return false;
            }

            $_SESSION[$sess_key] = in_array($cap, $this->imap_capabilities);
        }

        return $_SESSION[$sess_key];
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

        if ($enable && ($caps = $this->get_capability('THREAD'))) {
            $methods = array_intersect(array('REFS', 'REFERENCES', 'ORDEREDSUBJECT'), $caps);

            $this->threading = array_shift($methods);
        }

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

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        // mailbox exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        $search_conditions = NULL;
        if (is_array($this->search_set) && array_key_exists(0, $this->search_set)) {
            $search_conditions = $this->format_search_parameters($this->search_set[0]);
        }

        // set additional join tables according to supplied search / filter conditions
        $additional_joins = "";
        if (is_object($search_conditions) && property_exists($search_conditions, 'additional_join_tables')) {
            $additional_joins .= " {$search_conditions->additional_join_tables}";
        }

        // set where conditions according to supplied search / filter conditions
        $where_conditions = " WHERE 1 = 1 ";
        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_filter_str') && strlen($search_conditions->formatted_filter_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_filter_str} )";
        }

        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_search_str') && strlen($search_conditions->formatted_search_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_search_str} )";
        }

        // prepare base query
        $query = " SELECT COUNT(DISTINCT dbmail_messages.message_idnr) AS items_count "
                . " FROM dbmail_messages "
                . " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id AND dbmail_messages.mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

        //giuseppe
        if ($mode == 'UNSEEN') {
             $query .= " and seen_flag=0 ";
        }
           
        $query .= " {$additional_joins} ";
        $query .= " {$where_conditions} ";

        //giuseppe
        //console($query);
        
        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        $items_count = ($row['items_count'] > 0 ? $row['items_count'] : 0);

        // cache messages count and latest message id
        if ($status) {
            $this->set_folder_stats($folder, 'cnt', $items_count);
            $this->set_folder_stats($folder, 'maxuid', ($items_count ? $this->get_latest_message_idnr($folder) : 0));
        }

        //giuseppe
        //console("items_count: " . $items_count . "mode::: " . $mode);
        
        return $items_count;
    }

    /**
     * Get latest message ID within specific folder.
     *
     * @param  string  $folder  Folder name    
     * @return int     message_idnr
     */
    public function get_latest_message_idnr($folder = null) {

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        // mailbox exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        $search_conditions = NULL;
        if (is_array($this->search_set) && array_key_exists(0, $this->search_set)) {
            $search_conditions = $this->format_search_parameters($this->search_set[0]);
        }

        // set additional join tables according to supplied search / filter conditions
        $additional_joins = "";
        if (is_object($search_conditions) && property_exists($search_conditions, 'additional_join_tables')) {
            $additional_joins .= " {$search_conditions->additional_join_tables}";
        }

        // set where conditions according to supplied search / filter conditions
        $where_conditions = " WHERE 1 = 1 ";
        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_filter_str') && strlen($search_conditions->formatted_filter_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_filter_str} )";
        }

        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_search_str') && strlen($search_conditions->formatted_search_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_search_str} )";
        }

        // prepare base query
        $query = " SELECT MAX(message_idnr) AS latest_message_idnr "
                . " FROM dbmail_messages "
                . " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id AND dbmail_messages.mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

        $query .= " {$additional_joins} ";
        $query .= " {$where_conditions} ";

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

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        $mailbox_idnr = $this->get_mail_box_id($folder);

        $result = array();

        foreach ($uids as $uid) {

            // exec  signle query foreach messageId in $uids
            $query = " SELECT seen_flag, answered_flag, deleted_flag, flagged_flag, recent_flag, draft_flag "
                    . " FROM dbmail_messages "
                    . " WHERE message_idnr = {$this->dbmail->escape($uid)} "
                    . " AND mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

            $res = $this->dbmail->query($query);
            $row = $this->dbmail->fetch_assoc($res);

            if (!empty($row)) {

                $result[$uid] = array(
                    'seen' => ($row['seen_flag'] ? TRUE : FALSE),
                    'answered' => ($row['answered_flag'] ? TRUE : FALSE),
                    'deleted' => ($row['deleted_flag'] ? TRUE : FALSE),
                    'flagged' => ($row['flagged_flag'] ? TRUE : FALSE),
                    'recent' => ($row['recent_flag'] ? TRUE : FALSE),
                    'draft' => ($row['draft_flag'] ? TRUE : FALSE)
                );
            }
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

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

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

        if (!strlen($folder)) {
            $folder = $this->folder;
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
            $mail_box_idnr = $this->get_mail_box_id($folder_name);
            if ($mail_box_idnr) {
                $mail_box_idnr_list[] = $mail_box_idnr;
            }
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
            $mail_box_idnr = $this->get_mail_box_id($folder_name);
            if ($mail_box_idnr) {
                $mail_box_idnr_list[] = $mail_box_idnr;
            }
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

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        return $this->retrive_message($uid);
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

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        return $this->retrive_message($uid);
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

        $message_record = $this->get_message_record($uid);
        if (!$message_record) {
            return FALSE;
        }

        $physmessage_id = $message_record['physmessage_id'];
        $mime = $this->fetch_part_lists($physmessage_id);

        $mime_decoded = $this->decode_raw_message($mime->header . $mime->body);
        if (!$mime_decoded) {
            return FALSE;
        }

        return $this->get_message_part_body($mime_decoded, $part);
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


        // $this->retrive_message($uid);
        $rcube_message_header = $this->get_message_headers($uid);
        if (!$rcube_message_header) {
            // not found
            return FALSE;
        }

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

        // retrive message record
        $message_metadata = $this->get_message_record($uid);
        if (!$message_metadata) {
            // not found
            return FALSE;
        }

        // retrive physmessage record
        $physmessage_id = $message_metadata['physmessage_id'];
        $physmessage_metadata = $this->get_physmessage_record($physmessage_id);
        if (!$physmessage_metadata) {
            // not found
            return FALSE;
        }

        // retrive folder record
        $mailbox_idnr = $message_metadata['mailbox_idnr'];
        $folder_record = $this->get_folder_record($mailbox_idnr);
        if (!$folder_record) {
            // not found
            return FALSE;
        }

        // extract mime parts
        $mime = $this->fetch_part_lists($physmessage_id);

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

        // retrive message record
        $message_metadata = $this->get_message_record($uid);
        if (!$message_metadata) {
            // not found
            return FALSE;
        }

        // retrive physmessage record
        $physmessage_id = $message_metadata['physmessage_id'];
        $physmessage_metadata = $this->get_physmessage_record($physmessage_id);
        if (!$physmessage_metadata) {
            // not found
            return FALSE;
        }

        // retrive folder record
        $mailbox_idnr = $message_metadata['mailbox_idnr'];
        $folder_record = $this->get_folder_record($mailbox_idnr);
        if (!$folder_record) {
            // not found
            return FALSE;
        }

        // extract mime parts
        $mime = $this->fetch_part_lists($physmessage_id);

        return $mime->header;
    }

    /**
     * Sends the whole message source to stdout
     *
     * @param int  $uid       Message UID
     * @param bool $formatted Enables line-ending formatting
     */
    public function print_raw_body($uid, $formatted = true) {

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

        if (!strlen($folder)) {
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
        switch ($flag) {
            case 'UNDELETED':
                $flag_field = 'deleted_flag';
                $flag_value = 0;
                break;
            case 'DELETED':
                $flag_field = 'deleted_flag';
                $flag_value = 1;
                break;
            case 'SEEN':
                $flag_field = 'seen_flag';
                $flag_value = 1;
                break;
            case 'UNSEEN':
                $flag_field = 'seen_flag';
                $flag_value = 0;
                break;
            case 'FLAGGED':
                $flag_field = 'flagged_flag';
                $flag_value = 1;
                break;
            case 'UNFLAGGED':
                $flag_field = 'flagged_flag';
                $flag_value = 0;
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

            // set message flag
            $query = " UPDATE dbmail_messages "
                    . " SET {$this->dbmail->escape($flag_field)} = {$this->dbmail->escape($flag_value)} "
                    . " WHERE message_idnr = {$this->dbmail->escape($message_uid)} ";

            if (!$this->dbmail->query($query)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

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

        // retrive inserted ID
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

        // retrive inserted ID
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

        // store main mail headers (retrive real message headers instead of using Mail_mimeDecode content which are lowercased!!!!)
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



        // increment folder 'seq' flag
        if (!$this->increment_mailbox_seq($mailbox_idnr)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }



        /** MANCANO TUTTI LE CACHE DEGLI HEADER.... * */
        // funzione _header_cache in dm_message.c (FATTO)

        /** MANCA L'ENVELOPE CACHE * */
        // funzione imap_get_envelope in dm_misc.c
        // è un po' mostruosa ma alla fine facile
        // (VEDI tabella dbmail_envelope)) (FATTO)

        /** PENSO MANCHI IL REFERENCE FIELD * */
        // funzione dbmail_message_cache_referencesfield in dm_message.c
        // Generato a partire dall'header "References" oppure "In-Reply-To"
        // increment folder 'seq' flag (FATTO)

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

            // change mailbox and update 'seq' flag
            $query = "UPDATE dbmail_messages "
                    . " SET mailbox_idnr = {$this->dbmail->escape($to_mailbox_idnr)} "
                    . " WHERE message_idnr = {$this->dbmail->escape($message_uid)}";

            if (!$this->dbmail->query($query) || !$this->increment_message_seq($message_uid)) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        $this->increment_mailbox_seq($from_mailbox_idnr);
        $this->increment_mailbox_seq($to_mailbox_idnr);

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

            // retrive message record
            $message_metadata = $this->get_message_record($message_uid);
            if (!$message_metadata) {
                // not found
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // flag 'deleted' validation
            if ($message_metadata['deleted_flag']) {
                // don't copy deleted messages
                continue;
            }

            // retrive physmessage record
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

            // increment user quota
            if (!$this->increment_user_quota($this->user_idnr, $physmessage_metadata['messagesize'])) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        if (!$this->increment_mailbox_seq($to_mailbox_idnr)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // return status
        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    /**
     * Mark message(s) as deleted and expunge.
     *
     * @param mixed   $uids                 Message UIDs as array or comma-separated string, or '*'
     * @param string  $folder               Source folder
     * @param boolean $skip_transaction     Skip transaction to use this method from within an already opened transaction
     *
     * @return boolean True on success, False on error
     */
    public function delete_message($uids, $folder = NULL, $skip_transaction = FALSE) {

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        // folder exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        // format supplied message UIDs list
        $message_uids = $this->list_message_UIDs($uids, $folder);
        if (!$message_uids) {
            return FALSE;
        }

        // start transaction
        if (!$skip_transaction && !$this->dbmail->startTransaction()) {
            return FALSE;
        }

        foreach ($message_uids as $message_uid) {

            // retrive message record
            $message_metadata = $this->get_message_record($message_uid);
            if (!$message_metadata) {
                // not found
                if (!$skip_transaction) {
                    $this->dbmail->rollbackTransaction();
                }
                return FALSE;
            }

            // retrive physmessage record
            $physmessage_id = $message_metadata['physmessage_id'];
            $physmessage_metadata = $this->get_physmessage_record($physmessage_id);
            if (!$physmessage_metadata) {
                // not found
                if (!$skip_transaction) {
                    $this->dbmail->rollbackTransaction();
                }
                return FALSE;
            }

            $query = "DELETE FROM dbmail_messages "
                    . " WHERE  message_idnr = {$this->dbmail->escape($message_uid)}";

            if (!$this->dbmail->query($query)) {
                // rollbalk transaction
                if (!$skip_transaction) {
                    $this->dbmail->rollbackTransaction();
                }
                return FALSE;
            }

            // decrement user quota
            if (!$this->decrement_user_quota($this->user_idnr, $physmessage_metadata['messagesize'])) {
                if (!$skip_transaction) {
                    $this->dbmail->rollbackTransaction();
                }
                return FALSE;
            }
        }

        if (!$this->increment_mailbox_seq($mailbox_idnr)) {
            if (!$skip_transaction) {
                $this->dbmail->rollbackTransaction();
            }
            return FALSE;
        }


        if (!$skip_transaction && !$this->dbmail->endTransaction()) {
            return FALSE;
        } else {
            return TRUE;
        }
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

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        // folder exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        // format supplied message UIDs list - THIRD PARAMETER enable deleted_flag check
        $message_uids = $this->list_message_UIDs($uids, $folder, TRUE);
        if (!$message_uids) {
            return FALSE;
        }

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        foreach ($message_uids as $message_uid) {

            // retrive message record
            $message_metadata = $this->get_message_record($message_uid);
            if (!$message_metadata) {
                // not found
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // retrive physmessage record
            $physmessage_id = $message_metadata['physmessage_id'];
            $physmessage_metadata = $this->get_physmessage_record($physmessage_id);
            if (!$physmessage_metadata) {
                // not found
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            $query = "DELETE FROM dbmail_messages "
                    . " WHERE  message_idnr = {$this->dbmail->escape($message_uid)}";

            if (!$this->dbmail->query($query)) {
                // rollbalk transaction
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // decrement user quota
            if (!$this->decrement_user_quota($this->user_idnr, $physmessage_metadata['messagesize'])) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        if (!$this->increment_mailbox_seq($mailbox_idnr)) {

            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
    }

    /**
     * Parse message UIDs input
     *
     * @param mixed $uids UIDs array or comma-separated list or '*' or '1:*'
     *
     * @return array Two elements array with UIDs converted to list and ALL flag
     */
    protected function parse_uids($uids) {

        if ($uids === '*' || $uids === '1:*') {
            if (empty($this->search_set)) {
                $uids = '1:*';
                $all = true;
            }
            // get UIDs from current search set
            else {
                $uids = join(',', $this->search_set->get());
            }
        } else {
            if (is_array($uids)) {
                $uids = join(',', $uids);
            } else if (strpos($uids, ':')) {
                $uids = join(',', rcube_imap_generic::uncompressMessageSet($uids));
            }

            if (preg_match('/[^0-9,]/', $uids)) {
                $uids = '';
            }
        }

        return array($uids, (bool) $all);
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
    public function list_folders_subscribed($root = '', $name = '*', $filter = null, $rights = null, $skip_sort = false) {

        return $this->list_folders($root, $name, $filter, $rights, $skip_sort);
    }

    /**
     * Get a list of all folders available on the server.
     *
     * @param string  $root      IMAP root dir
     * @param string  $name      Optional name pattern
     * @param mixed   $filter    Optional filter
     * @param string  $rights    Optional ACL requirements
     * @param bool    $skip_sort Enable to return unsorted list (for better performance)
     *
     * @return array Indexed array with folder names
     */
    public function list_folders($root = '', $name = '*', $filter = null, $rights = null, $skip_sort = false) {

        $folders = array();

        // 'INBOX' should always be available
        $folders[] = 'INBOX';

        // get 'special' folders
        $special_folders = $this->get_special_folders();
        foreach ($special_folders as $special_folder) {
            $folders[] = $special_folder;
        }

        // get 'user' forlders
        $query = "SELECT name "
                . " FROM dbmail_mailboxes "
                . " WHERE owner_idnr = {$this->dbmail->escape($this->user_idnr)} "
                . " AND deleted_flag = 0 ";

        if (!$skip_sort) {
            $query .= " ORDER BY name ASC ";
        }

        $res = $this->dbmail->query($query);

        while ($row = $this->dbmail->fetch_assoc($res)) {
            if (!in_array($row['name'], $folders)) {
                $folders[] = $row['name'];
            }
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

            // subscription already exists?
            $query = "SELECT mailbox_id "
                    . " FROM dbmail_subscription "
                    . " WHERE user_id = {$this->dbmail->escape($this->user_idnr)} "
                    . " AND mailbox_id = {$this->dbmail->escape($mailbox_idnr)}";

            $res = $this->dbmail->query($query);

            // num_rows
            if ($this->dbmail->num_rows($res) == 0) {
                // subscription doesn't exists - create it

                $query = "INSERT INTO dbmail_subscription "
                        . " ("
                        . "      user_id, "
                        . "      mailbox_id"
                        . " ) "
                        . " VALUES "
                        . " ("
                        . "      {$this->dbmail->escape($this->user_idnr)}, "
                        . "      {$this->dbmail->escape($mailbox_idnr)} "
                        . " )";

                if (!$this->dbmail->query($query)) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }
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

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        // prepare query
        $query = " INSERT INTO dbmail_mailboxes "
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
        if (!$this->dbmail->query($query)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // subscription management (if needed)
        if ($subscribe) {

            // extract created mailbox id
            $mailbox_idnr = $this->dbmail->insert_id('dbmail_mailboxes');
            if (!$mailbox_idnr || strlen($mailbox_idnr) == 0) {
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            // add subscription (don't use 'subscribe' method to avoid transaction issues)
            $query = "INSERT INTO dbmail_subscription "
                    . " ("
                    . "      user_id, "
                    . "      mailbox_id"
                    . " ) "
                    . " VALUES "
                    . " ("
                    . "      {$this->dbmail->escape($this->user_idnr)}, "
                    . "      {$this->dbmail->escape($mailbox_idnr)} "
                    . " )";

            if (!$this->dbmail->query($query)) {
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

                // rename sub-folder
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

        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
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

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        // get mailbox sub folders
        $sub_folders = $this->get_sub_folders($folder);

        // has children?
        if (count($sub_folders) > 0) {

            // delete children
            foreach ($sub_folders as $sub_folder_idnr => $sub_folder_name) {

                // delete sub folder messages
                if (!$this->delete_message('*', $sub_folder_name, TRUE)) {
                    // error while deleting subfolder messages
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }

                // delete sub folder
                $query = "DELETE FROM dbmail_mailboxes "
                        . " WHERE mailbox_idnr = {$this->dbmail->escape($sub_folder_idnr)} ";

                if (!$this->dbmail->query($query)) {
                    // rollbalk transaction
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }
            }
        }

        // delete folder messages
        if (!$this->delete_message('*', $folder, TRUE)) {
            // error while deleting folder messages
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        // delete folder
        $query = "DELETE FROM dbmail_mailboxes "
                . " WHERE mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

        if (!$this->dbmail->query($query)) {
            // rollbalk transaction
            $this->dbmail->rollbackTransaction();
            return FALSE;
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

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        // folder exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            // not found
            return FALSE;
        }

        // retrive folder record
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

        $folderAttributes = $this->folder_attributes($folder);
        $folderRights = $this->get_acl($folder);

        $options = array(
            'is_root' => FALSE,
            'name' => $folder,
            'attributes' => $folderAttributes,
            'namespace' => $this->folder_namespace($folder),
            'special' => (in_array($folder, self::$folder_types) ? TRUE : FALSE),
            'noselect' => (array_key_exists('no_select', $folderAttributes) ? $folderAttributes['no_select'] : FALSE),
            'rights' => $folderRights,
            'norename' => (in_array('d', $folderRights) ? FALSE : TRUE)
        );

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

        if (!strlen($folder)) {
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

        if (!strlen($folder)) {
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
        if (!isset($this->icache['special-folders'])) {
            $rcube = rcube::get_instance();
            $this->icache['special-folders'] = array();

            foreach (self::$folder_types as $type) {
                if ($folder = $rcube->config->get($type . '_mbox')) {
                    $this->icache['special-folders'][$type] = $folder;
                }
            }
        }

        return $this->icache['special-folders'];
    }

    /**
     * Set special folder associations stored in backend
     */
    public function set_special_folders($specials) {
        // should be overriden by storage class if backend supports special folders (SPECIAL-USE)
        unset($this->icache['special-folders']);
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

        // folder exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        // user exists?
        $user_idnr = $this->get_user_id($user);
        if (!$user_idnr) {
            return FALSE;
        }

        // explode acls string to array
        $exploded_acls = str_split($acl);

        $query = "INSERT INTO dbmail_acl "
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
                . " )";

        return $this->dbmail->query($query);
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

        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        $user_idnr = $this->get_user_id($user);
        if (!$user_idnr) {
            return FALSE;
        }

        $query = "DELETE FROM dbmail_acl"
                . " WHERE user_id = {$this->dbmail->escape($mailbox_idnr)} "
                . " AND mailbox_id = {$this->dbmail->escape($user_idnr)} ";

        return $this->dbmail->query($query);
    }

    /**
     * Returns the access control list for a folder (GETACL).
     *
     * @param string $folder Folder name
     *
     * @return array User-rights array on success, NULL on error
     */
    public function get_acl($folder) {

        /*
         *  AL MOMENTO NON GESTIAMO LE CONDIVISIONI (vedi tabella 'dbmail_acl')
         * lookup_flag = l
         * read_flag = r
         * seen_flag = s
         * write_flag = w
         * insert_flag = i
         * post_flag = p
         * create_flag = c
         * delete_flag = d
         * administer_flag = a
         */
        $acls = array();
        $acls[] = 'l';
        $acls[] = 'r';
        $acls[] = 's';
        $acls[] = 'w';
        $acls[] = 'i';
        $acls[] = 'p';
        $acls[] = 'c';
        $acls[] = 'd';
        $acls[] = 'a';

        return $acls;
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
        // TO DO!!!!!
    }

    /**
     * Returns the set of rights that the current user has to a folder (MYRIGHTS).
     *
     * @param string $folder Folder name
     *
     * @return array MYRIGHTS response on success, NULL on error
     */
    public function my_rights($folder) {
        // TO DO!!!!!
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
     * Clears the cache.
     *
     * @param string  $key         Cache key name or pattern
     * @param boolean $prefix_mode Enable it to clear all keys starting
     *                             with prefix specified in $key
     */
    public function clear_cache($key = null, $prefix_mode = false) {
        // TO DO!!!!!
    }

    /**
     * Returns cached value
     *
     * @param string $key Cache key
     *
     * @return mixed Cached value
     */
    public function get_cache($key) {
        // TO DO!!!!!
    }

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

        $this->dbmail->db_connect('r');

        return(!is_null($this->dbmail->is_error()) ? FALSE : TRUE);
    }

    /**
     * Retrive mailbox identifier
     *
     * @param string  $folder    folder name
     *
     * @return int mailbox_idnr on success, False on failure
     */
    protected function get_mail_box_id($folder) {

        $query = " SELECT mailbox_idnr "
                . " FROM dbmail_mailboxes "
                . " WHERE owner_idnr = '{$this->dbmail->escape($this->user_idnr)}' "
                . " AND deleted_flag = 0 "
                . " AND name = '{$this->dbmail->escape($folder)}' ";

        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        if (!is_array($row) || !array_key_exists('mailbox_idnr', $row)) {
            return FALSE;
        }

        return $row['mailbox_idnr'];
    }

    /**
     * Retrive physmessage id
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
     * Retrive user identifier
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
     * retrive message record
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
     * retrive physmessage record
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
     * retrive folder record
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
     * Retrive mailbox sub folders
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
     * retrive message headers from 'dbmail_mimeparts'
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
     * Retrive delimiter for supplied header name
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

        $header = trim($header);

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

                    // remove trailing / leading quotes from $value
                    $value = trim($value, "'");

                    // remove trailing / leading double quotes from $value
                    $value = trim($value, "\"");

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

        $query = "SELECT id "
                . "FROM dbmail_headername "
                . "WHERE headername = '{$this->dbmail->escape($header_name)}' "
                . "LIMIT 1";

        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        return (is_array($row) && array_key_exists('id', $row) ? $row['id'] : FALSE);
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
     * fetch message
     * @param int $message_idnr
     * @param rcube_message_header 
     */
    private function retrive_message($message_idnr) {

        // retrive message record
        $message_metadata = $this->get_message_record($message_idnr);
        if (!$message_metadata) {
            // not found
            return FALSE;
        }

        // retrive physmessage record
        $physmessage_id = $message_metadata['physmessage_id'];
        $physmessage_metadata = $this->get_physmessage_record($physmessage_id);
        if (!$physmessage_metadata) {
            // not found
            return FALSE;
        }

        // retrive folder record
        $mailbox_idnr = $message_metadata['mailbox_idnr'];
        $folder_record = $this->get_folder_record($mailbox_idnr);
        if (!$folder_record) {
            // not found
            return FALSE;
        }

        // extract mime parts
        $mime = $this->fetch_part_lists($physmessage_id);

        // prepare response
        $rcmh = new rcube_message_header();
        $rcmh->id = $message_idnr;
        $rcmh->uid = $message_idnr;
        $rcmh->folder = $folder_record['name'];
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
        $rcmh->size = $physmessage_metadata['messagesize'];
        $rcmh->timestamp = time();
        $rcmh->flags["SEEN"] = ($message_metadata['seen_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["ANSWERED"] = ($message_metadata['answered_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["DELETED"] = ($message_metadata['deleted_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["FLAGGED"] = ($message_metadata['flagged_flag'] == 1 ? TRUE : FALSE);

        $mime_decoded = $this->decode_raw_message($mime->header . $mime->body);
        if (!$mime_decoded) {
            return FALSE;
        }

        $rcmh->structure = $this->get_structure($mime_decoded);

        return $rcmh;
    }

    /**
     * Increment 'seq' flag for supplied mailbox ID
     *
     * @param int  $mailbox_idnr    mailbox ID
     *
     * @return boolean True on success, False on failure
     */
    private function increment_mailbox_seq($mailbox_idnr) {

        $query = "UPDATE dbmail_mailboxes "
                . " SET seq = (seq + 1) "
                . " WHERE mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)}";

        return ($this->dbmail->query($query) ? TRUE : FALSE);
    }

    /**
     * Increment 'seq' flag for supplied message ID
     *
     * @param int  $message_idnr    message ID
     *
     * @return boolean True on success, False on failure
     */
    private function increment_message_seq($message_idnr) {

        $query = "UPDATE dbmail_messages "
                . " SET seq = (seq + 1) "
                . " WHERE message_idnr = {$this->dbmail->escape($message_idnr)}";

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

        if (is_string($uids) && $uids == '*' && strlen($folder) > 0) {

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

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        // set current mailbox folder ID
        $mailbox_idnr = $this->get_mail_box_id($folder);

        // validate sort order (use default when not supplied)
        $sort_order = (strtoupper($sort_order) == 'DESC' ? 'DESC' : 'ASC');

        // set query offset / limit
        $page = ((int) $page > 0 ? $page : $this->list_page);
        $query_offset = ($page > 0 ? (($page - 1) * $this->page_size) : 0);
        $query_limit = $this->page_size;

        // set additional join tables according to supplied search / filter conditions
        $additional_joins = "";
        if (is_object($search_conditions) && property_exists($search_conditions, 'additional_join_tables')) {
            $additional_joins .= " {$search_conditions->additional_join_tables}";
        }

        // set where conditions according to supplied search / filter conditions
        $where_conditions = " WHERE 1 = 1 ";
        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_filter_str') && strlen($search_conditions->formatted_filter_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_filter_str} )";
        }

        if (is_object($search_conditions) && property_exists($search_conditions, 'formatted_search_str') && strlen($search_conditions->formatted_search_str) > 0) {
            $where_conditions .= " AND ( {$search_conditions->formatted_search_str} )";
        }

        // set additional join tables depending by supplied sort conditions
        switch ($sort_field) {
            case 'subject':
            case 'from':
            case 'date':
                // 'subject' / 'from' and 'date' values are stored into 'dbmail_headervalue' table
                $header_id = $this->get_header_id_by_header_name($sort_field);

                $additional_joins .= " "
                        . " LEFT JOIN dbmail_header AS sort_dbmail_header ON dbmail_physmessage.id = sort_dbmail_header.physmessage_id AND sort_dbmail_header.headername_id = {$this->dbmail->escape($header_id)} "
                        . " LEFT JOIN dbmail_headervalue AS sort_dbmail_headervalue ON sort_dbmail_header.headervalue_id = sort_dbmail_headervalue.id ";

                $sort_condition = " ORDER BY sort_dbmail_headervalue.sortfield {$this->dbmail->escape($sort_order)} ";
                break;
            case 'size':
                // 'size' value is stored into 'dbmail_physmessage' table - no additional joins needed
                $sort_condition = " ORDER BY dbmail_physmessage.messagesize {$this->dbmail->escape($sort_order)} ";
                break;
            default:
                // natural sort - no sort  needed
                break;
        }

        // prepare base query
        $query = " SELECT DISTINCT dbmail_messages.message_idnr, dbmail_messages.physmessage_id, "
                . " dbmail_physmessage.messagesize, dbmail_messages.seen_flag, "
                . " dbmail_messages.answered_flag, dbmail_messages.deleted_flag, "
                . " dbmail_messages.flagged_flag "
                . " FROM dbmail_messages "
                . " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id AND dbmail_messages.mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

        $query .= " {$additional_joins} ";
        $query .= " {$where_conditions} ";
        $query .= " {$sort_condition} ";
        $query .= " LIMIT {$this->dbmail->escape($query_offset)}, {$this->dbmail->escape($query_limit)} ";

        $res = $this->dbmail->query($query);

        $headers = array();
        $msg_index = $query_offset++;
        $toSess = array();

        while ($msg = $this->dbmail->fetch_assoc($res)) {

            $message_idnr = $msg['message_idnr'];
            $physmessage_id = $msg['physmessage_id'];
            $messagesize = $msg['messagesize'];
            $seen = $msg['seen_flag'];
            $answered = $msg['answered_flag'];
            $deleted = $msg['deleted_flag'];
            $flagged = $msg['flagged_flag'];

            $message_headers = $this->get_physmessage_headers($physmessage_id);

            $imploded_headers = '';
            foreach ($message_headers as $header_name => $header_value) {
                $imploded_headers .= $header_name . $this->get_header_delimiter($header_name) . $header_value . "\n";
            }

            $rcmh = new rcube_message_header();
            $rcmh->id = $msg_index;
            $rcmh->uid = $message_idnr;
            $rcmh->ctype = $this->get_header_value($imploded_headers, 'content-type');
            $rcmh->folder = $folder;
            $rcmh->subject = $this->get_header_value($imploded_headers, 'subject');
            $rcmh->from = $this->get_header_value($imploded_headers, 'from');
            $rcmh->to = $this->get_header_value($imploded_headers, 'to');
            $rcmh->replyto = $this->get_header_value($imploded_headers, 'return-path');
            $rcmh->in_reply_to = $this->get_header_value($imploded_headers, 'return-path');
            $rcmh->date = $this->get_header_value($imploded_headers, 'date');
            $rcmh->internaldate = $this->get_header_value($imploded_headers, 'date');
            $rcmh->messageID = "mid:" . $msg_index;
            $rcmh->size = $messagesize;
            $rcmh->timestamp = time();
            $rcmh->flags["SEEN"] = $seen;
            $rcmh->flags["ANSWERED"] = $answered;
            $rcmh->flags["DELETED"] = $deleted;
            $rcmh->flags["FLAGGED"] = $flagged;

            $headers[$msg_index] = $rcmh;

            $toSess[$rcmh->uid] = $physmessage_id . ":" . $msg_index . ":" . $messagesize . ":" . $seen . ":" . $answered . ":" . $deleted . ":" . $flagged;

            $msg_index++;
        }

        $_SESSION['dbmail_header'] = $toSess;

        return $headers;
    }

    /**
     * Decode supplied raw message using library PEAR Mail_mimeDecode
     * @param string $raw_message
     * @return Mail_mimeDecode
     */
    private function decode_raw_message($raw_message, $decode_bodies = TRUE) {

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

        // add mime_id attributes to '$decoded' array items (pass by reference)
        $mime_decode->getMimeNumbers($decoded);

        return $decoded;
    }

    /**
     * create raw message from part lists
     * @param $physmessage_id
     * @return stdClass
     */
    private function fetch_part_lists($physmessage_id) {

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

//            console("Depth ".$depth." [".$prevdepth."] - Header ".$is_header." [".$prev_header."]");
                        
            if ($is_header) {
                $prev_boundary = $got_boundary;
//                $prev_is_message = $is_message;

                $is_message = preg_match('~content-type:\s+message/rfc822\b~i', $blob);
                
                
//                console("111111->". $blob);
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
                //console("È finito la parte, torniamo su");
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


                //console("È finito il body!");

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

        
        
        
//        console("3333333->". $body);
        
        $response = new stdClass();
        $response->header = $header;
        $response->body = $body;

        
        
        //console($response->body);
        
        
        
        return $response;
    }

    
    
    
    

    private function get_structure($structure) {

        //console($structure);
        //arrivasolo uno
        
        
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
     * Return passed mime part
     * @param stdClass $mime_decoded
     * @param string $mime_id
     * @return stdClass on success, False if not found
     */
    private function get_message_part_body($mime_decoded, $mime_id) {

        $response = FALSE;

        if (property_exists($mime_decoded, 'mime_id') && $mime_decoded->mime_id == $mime_id) {
            // found
            $response = (property_exists($mime_decoded, 'body') ? $mime_decoded->body : FALSE);
        } elseif (property_exists($mime_decoded, 'parts')) {
            // fetch children
            foreach ($mime_decoded->parts as $part) {

                $response = $this->get_message_part_body($part, $mime_id);

                if ($response) {
                    // found
                    break;
                }
            }
        }


        return $response;
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

        ## Enable debug as you please by changing this to TRUE
        if (FALSE) {
            console("Part Insert, physmessage id: " . $physmessage_id);
            console("Part Insert, is header:      " . $is_header);
            console("Part Insert, part key:       " . $part_key);
            console("Part Insert, part depth:     " . $part_depth);
            console("Part Insert, part order:     " . $part_order);
            console("Part Insert, hash:           " . $hash);
        }

        // blob exists?
        $query = "SELECT id "
                . " FROM dbmail_mimeparts "
                . " WHERE hash = '{$this->dbmail->escape($hash)}' "
                . " AND size = {$this->dbmail->escape(strlen($data))}";

        $result = $this->dbmail->query($query);

        if ($this->dbmail->num_rows($result) == 0) {

            // blob not found - insert new record in 'dbmail_mimeparts'
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

            // retrive inserted ID
            $part_id = $this->dbmail->insert_id('dbmail_mimeparts');
            if (!$part_id) {
                return FALSE;
            }
        } else {
            // blob found - use current record ID
            $row = $this->dbmail->fetch_assoc($result);
            $part_id = $row["id"];
        }

        // register 'dbmail_partlists' to 'dbmail_mimeparts' relation
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
     * Function to exract the RAW Headers of a message
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
            if (strlen(trim($line)) > 0)
                $raw_header .= $line . "\n";
            else
                break;
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
          console("Store mime object");
          console($mime_decoded);
          console("Store mime object - part key:   " . $part_key);
          console("Store mime object - part depth: " . $part_depth);
          console("Store mime object - part order: " . $part_order);
         */
        // Top level headers (depth = 0) are taken directly from the message envelope
        if ($part_depth > 0 && property_exists($mime_decoded, 'headers')) {

            //Console("We have an header");

            $part_key++;

            $headers = '';
            foreach ($mime_decoded->headers as $header_name => $header_value) {

                // Headers have a specific CASE-matching rule...
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

        // Do we have a body?
        if (property_exists($mime_decoded, 'body')) {
            //Console("We have a message");
            if (!$this->_part_insert($physmessage_id, $mime_decoded->body, 0, $part_key, $part_depth, $part_order)) {
                return FALSE;
            }
        } else {
            if ($part_depth == 0) {
                //Console("Empty body for first level");
                if (!$this->_part_insert($physmessage_id, "This is a multi-part message in MIME format.", 0, $part_key, $part_depth, $part_order)) {
                    return FALSE;
                }
            }
        }

        // Do we have additional parts?
        if (property_exists($mime_decoded, 'parts')) {
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
     * Retrive searchable headers key / pairs
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

        // add new header names to 'dbmail_headername' table?
        $dbmail_fixed_headername_cache = $this->rcubeInstance->config->get('dbmail_fixed_headername_cache', null);

        // retrive $header_name_id (if exists)
        $header_name_id = $this->get_header_id_by_header_name($header_name);

        // retrive $header_value_id (if exists)
        $header_value_id = $this->get_header_value_id_by_header_value($header_value);

        if (!$dbmail_fixed_headername_cache && !$header_name_id) {
            // header name doesn't exists and we don't want to add extra headers - OK
            return TRUE;
        }

        // fix missing header_name reference (if needed)
        if (!$header_name_id) {
            // header name doesn't exists - create it
            $query = "INSERT INTO dbmail_headername "
                    . " ( "
                    . "    headername "
                    . " )"
                    . " VALUES "
                    . " ( "
                    . "    '{$this->dbmail->escape($header_name)}' "
                    . " )";


            if (!$this->dbmail->query($query)) {
                return FALSE;
            }

            // retrive inserted ID
            $header_name_id = $this->dbmail->insert_id('dbmail_headername');
            if (!$header_name_id) {
                return FALSE;
            }
        }

        // fix missing header_value reference (if needed)
        if (!$header_value_id) {

            $date = DateTime::createFromFormat('Y-m-d H:i:s', $header_value);
            $escaped_date_field = ($date ? "'{$this->dbmail->escape($date->format('Y-m-d H:i:s'))}'" : "NULL");

            // header value doesn't exists - create it
            $query = "INSERT INTO dbmail_headervalue "
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
                    . "    '{$this->dbmail->escape($header_value)}', "
                    . "     {$escaped_date_field} "
                    . " )";


            if (!$this->dbmail->query($query)) {
                return FALSE;
            }

            // retrive inserted ID
            $header_value_id = $this->dbmail->insert_id('dbmail_headervalue');
            if (!$header_value_id) {
                return FALSE;
            }
        }

        // add dbmail_headername to dbmail_headervalue relation
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
     * Retrive mail envelope headers
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

        if (count($envelope_headers) == 0) {
            // nothing supplied
            return TRUE;
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
     * @TODO: move to separate DB table (cache?)
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
        if ($_SESSION['folders'][$folder]) {
            return (array) $_SESSION['folders'][$folder];
        }

        return array();
    }

}
