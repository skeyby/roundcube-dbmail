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
 * 
 * !!! IMPORTANT !!!
 * Use the official PEAR Mail_mimeDecode library, changing following line in 'composer.json'
 * change  "pear/mail_mime-decode": ">=1.5.5",
 * to      "pear-pear.php.net/Mail_mimeDecode": ">=1.5.5",
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
                /*
                 * TEST MD5 LOGIN!!!!!!!!
                 */
                $salt = substr($row['passwd'], 0, (strrpos($row['passwd'], '$') + 1));
                $valid_user = (crypt($pass, $salt) != $row['passwd']);
                break;
            case 'md5sum':
                $valid_user = (md5($pass) == $row['passwd']);
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

        $this->search_set = $set;
    }

    /**
     * Return the saved search set.
     *
     * @return array Search set in driver specific format, NULL if search wasn't initialized
     */
    public function get_search_set() {

        return $this->search_set;
    }

    /**
     * Returns the storage server's (IMAP) capability
     *
     * @param   string  $cap Capability name
     *
     * @return  mixed   Capability value or TRUE if supported, FALSE if not
     */
    public function get_capability($cap) {
        // TO DO!!!!!!!
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
            $methods = array('REFS', 'REFERENCES', 'ORDEREDSUBJECT');
            $methods = array_intersect($methods, $caps);

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
        // TO DO!!!!
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

        $where_condition = '';
        switch ($mode) {
            case 'ALL':
                // no filters applied
                $where_condition = '';
                break;
            case 'THREADS':
                // TO DO!!!!!
                $where_condition = '';
                break;
            case 'UNSEEN':
                $where_condition = " and seen_flag = 0 ";
                break;
            case 'RECENT':
                $where_condition = " and recent_flag = 1 ";
                break;
            case 'EXISTS':
                // TO DO!!!!!
                $where_condition = '';
                break;
        }

        $query = "SELECT count(*) as items_count "
                . " FROM dbmail_messages "
                . " WHERE mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} "
                . " {$where_condition} ";

        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        return ($row['items_count'] > 0 ? $row['items_count'] : FALSE);
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
                    'seen' => $row['seen_flag'],
                    'answered' => $row['answered_flag'],
                    'deleted' => $row['deleted_flag'],
                    'flagged' => $row['flagged_flag'],
                    'recent' => $row['recent_flag'],
                    'draft' => $row['draft_flag']
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
        // TO DO!!!!
    }

    /**
     * Refresh saved search set
     *
     * @return array Current search set
     */
    public function refresh_search() {
        // TO DO!!!!
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

        // message exists?
        $query = " SELECT * "
                . " FROM dbmail_messages "
                . " WHERE message_idnr = '{$this->dbmail->escape($uid)}' ";

        $res = $this->dbmail->query($query);
        if ($this->dbmail->num_rows($res) == 0) {
            // not found
            return FALSE;
        }

        $message_metadata = $this->dbmail->fetch_assoc($res);

        $physmessage_id = $message_metadata['physmessage_id'];

        // extract mime parts
        $mime = $this->fetch_part_lists($physmessage_id);

        // prepare response
        $rcmh = new rcube_message_header();
        $rcmh->id = $uid;
        $rcmh->uid = $uid;
        $rcmh->folder = $message_metadata['mailbox_name'];
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
        $rcmh->size = $message_metadata['messagesize'];
        $rcmh->timestamp = time();
        $rcmh->flags["SEEN"] = ($message_metadata['seen_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["ANSWERED"] = ($message_metadata['answered_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["DELETED"] = ($message_metadata['deleted_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["FLAGGED"] = ($message_metadata['flagged_flag'] == 1 ? TRUE : FALSE);
        $rcmh->bodystructure = array();

        $mime_decode = new Mail_mimeDecode($mime->header . "\r\n" . $mime->body);

        $decode_params = array(
            'include_bodies' => TRUE,
            'decode_bodies' => TRUE,
            'decode_headers' => TRUE,
            'rfc_822bodies' => TRUE
        );

        $rcmh->structure = $mime_decode->decode($decode_params);

        return $rcmh;
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

        $headers = array();

        // get message metadata
        $query = "SELECT dbmail_messages.seen_flag, dbmail_messages.answered_flag, "
                . " dbmail_messages.deleted_flag, dbmail_messages.flagged_flag, "
                . " dbmail_physmessage.id as physmessage_id, dbmail_physmessage.messagesize,"
                . " dbmail_mailboxes.name as mailbox_name "
                . " FROM dbmail_messages "
                . " INNER JOIN dbmail_mailboxes on dbmail_messages.mailbox_idnr = dbmail_mailboxes.mailbox_idnr "
                . " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id "
                . " WHERE dbmail_messages.message_idnr = {$this->dbmail->escape($uid)} ";

        $message_metadata_result = $this->dbmail->query($query);
        if ($this->dbmail->num_rows($message_metadata_result) == 0) {
            // record doesn't exists
            return FALSE;
        }

        $message_metadata = $this->dbmail->fetch_assoc($message_metadata_result);

        // get message header
        $query = "SELECT dbmail_header.headername_id, dbmail_header.headervalue_id "
                . " FROM dbmail_physmessage "
                . " INNER JOIN dbmail_header on dbmail_physmessage.id = dbmail_header.physmessage_id "
                . " WHERE dbmail_header.physmessage_id = {$this->dbmail->escape($message_metadata['physmessage_id'])} ";
        $message_headers_result = $this->dbmail->query($query);

        if ($this->dbmail->num_rows($message_headers_result) == 0) {
            // record doesn't exists
            return FALSE;
        }

        while ($message_header = $this->dbmail->fetch_assoc($message_headers_result)) {

            // get header name
            $query = "SELECT headername"
                    . " FROM dbmail_headername "
                    . " WHERE id = {$this->dbmail->escape($message_header['headername_id'])}";

            $header_name_result = $this->dbmail->query($query);
            $name = $this->dbmail->fetch_assoc($header_name_result);
            $header_name = $name['headername'];

            // get header value
            $query = "SELECT headervalue"
                    . " FROM dbmail_headervalue "
                    . " WHERE id = {$this->dbmail->escape($message_header['headervalue_id'])}";

            $header_value_result = $this->dbmail->query($query);
            $value = $this->dbmail->fetch_assoc($header_value_result);
            $header_value = $value['headervalue'];

            $headers[$header_name] = $header_value;
        }

        $rcmh = new rcube_message_header();
        $rcmh->id = $uid;
        $rcmh->uid = $uid;
        $rcmh->folder = $message_metadata['mailbox_name'];
        $rcmh->subject = (array_key_exists('subject', $headers) ? $headers['subject'] : NULL);
        $rcmh->from = (array_key_exists('from', $headers) ? $headers['from'] : NULL);
        $rcmh->to = (array_key_exists('to', $headers) ? $headers['to'] : NULL);
        $rcmh->cc = (array_key_exists('cc', $headers) ? $headers['cc'] : NULL);
        $rcmh->bcc = (array_key_exists('bcc', $headers) ? $headers['bcc'] : NULL);
        $rcmh->replyto = (array_key_exists('reply-to', $headers) ? $headers['reply-to'] : NULL);
        $rcmh->in_reply_to = (array_key_exists('in-reply-to', $headers) ? $headers['in-reply-to'] : NULL);
        $rcmh->ctype = (array_key_exists('content-type', $headers) ? $headers['content-type'] : NULL);
        $rcmh->references = (array_key_exists('references', $headers) ? $headers['references'] : NULL);
        $rcmh->mdn_to = (array_key_exists('return-receipt-to', $headers) ? $headers['return-receipt-to'] : NULL);
        $rcmh->priority = (array_key_exists('x-priority', $headers) ? $headers['x-priority'] : NULL);
        $rcmh->date = (array_key_exists('date', $headers) ? $headers['date'] : NULL);
        $rcmh->internaldate = (array_key_exists('date', $headers) ? $headers['date'] : NULL);
        $rcmh->messageID = (array_key_exists('message-id', $headers) ? $headers['message-id'] : NULL);
        $rcmh->size = $message_metadata['messagesize'];
        $rcmh->timestamp = time();
        $rcmh->flags["SEEN"] = ($message_metadata['seen_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["ANSWERED"] = ($message_metadata['answered_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["DELETED"] = ($message_metadata['deleted_flag'] == 1 ? TRUE : FALSE);
        $rcmh->flags["FLAGGED"] = ($message_metadata['flagged_flag'] == 1 ? TRUE : FALSE);
        $rcmh->bodystructure = array();

        return $rcmh;
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

        $headers = '';
        $size = 0;
        $body = '';

        $query = "SELECT dbmail_partlists.is_header, dbmail_mimeparts.data, dbmail_mimeparts.size "
                . " FROM dbmail_messages "
                . " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id "
                . " INNER JOIN dbmail_partlists ON dbmail_physmessage.id = dbmail_partlists.physmessage_id "
                . " INNER JOIN dbmail_mimeparts ON dbmail_partlists.part_id = dbmail_mimeparts.id "
                . " WHERE dbmail_messages.message_idnr = {$this->dbmail->escape($uid)} "
                . " AND dbmail_partlists.part_key = {$this->dbmail->escape($part)} "
                . " ORDER BY part_key ASC, part_order ASC, is_header DESC ";

        $res = $this->dbmail->query($query);

        while ($row = $this->dbmail->fetch_assoc($res)) {

            if ($row['is_header']) {
                $headers = $row['data'];
                $size = $row['size'];
            } else {
                $body .= $row['data'];
            }
        }

        $o_part = new rcube_message_part;
        $o_part->ctype_primary = $this->get_header_value($headers, 'Content-Type');
        $o_part->encoding = $this->get_header_value($headers, 'Content-Transfer-Encoding');
        $o_part->charset = $this->get_header_value($headers, 'charset');
        $o_part->size = $size;

        if ($fp || $print) {
            return true;
        }

        // convert charset (if text or message part)
        if ($body && preg_match('/^(text|message)$/', $o_part->ctype_primary)) {
            // Remove NULL characters if any (#1486189)
            if ($formatted && strpos($body, "\x00") !== false) {
                $body = str_replace("\x00", '', $body);
            }

            if (!$skip_charset_conv) {
                if (!$o_part->charset || strtoupper($o_part->charset) == 'US-ASCII') {
                    // try to extract charset information from HTML meta tag (#1488125)
                    if ($o_part->ctype_secondary == 'html' && preg_match('/<meta[^>]+charset=([a-z0-9-_]+)/i', $body, $m)) {
                        $o_part->charset = strtoupper($m[1]);
                    } else {
                        $o_part->charset = $this->default_charset;
                    }
                }
                $body = rcube_charset::convert($body, $o_part->charset);
            }
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
        $headers = $this->get_message_headers($uid);
        return rcube_charset::convert($this->get_message_part($uid, $part, null), $headers->charset ? $headers->charset : $this->default_charset);
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
        // TO DO!!!!!!
    }

    /**
     * Returns the message headers as string
     *
     * @param int $uid  Message UID
     *
     * @return string Message headers string
     */
    public function get_raw_headers($uid) {
        // TO DO!!!!!!
    }

    /**
     * Sends the whole message source to stdout
     *
     * @param int  $uid       Message UID
     * @param bool $formatted Enables line-ending formatting
     */
    public function print_raw_body($uid, $formatted = true) {
        // TO DO!!!!!!
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
     *
     * @return int|bool Appended message UID or True on success, False on error
     */
    public function save_message($folder, &$message, $headers = '', $is_file = false, $flags = array(), $date = null) {

        /*
          console($folder);
          console($message);
          console($headers);
          console($is_file);
          console($flags);
          console($date);

          [27-Feb-2015 11:33:21 +0100]: <s2l5e7bj> Drafts
          [27-Feb-2015 11:33:21 +0100]: <s2l5e7bj> MIME-Version: 1.0
          Date: Fri, 27 Feb 2015 11:33:21 +0100
          From: utente1@mail.qa.schema31.it
          To: undisclosed-recipients:;
          Subject:
          Message-ID: <9116000ffb79fa7e417a9df0f2a853fc@mail.qa.schema31.it>
          X-Sender: utente1@mail.qa.schema31.it
          User-Agent: Roundcube Webmail/1.1.0


          [27-Feb-2015 11:33:21 +0100]: <s2l5e7bj>
          [27-Feb-2015 11:33:21 +0100]: <s2l5e7bj> false
          [27-Feb-2015 11:33:21 +0100]: <s2l5e7bj> array (
          0 => 'SEEN',
          )
          [27-Feb-2015 11:33:21 +0100]: <s2l5e7bj> NULL
         */


        $params = array(
            'include_bodies' => TRUE,
            'decode_bodies' => TRUE,
            'decode_headers' => TRUE,
            'rfc_822bodies' => TRUE
        );

        $decode = new Mail_mimeDecode($message, "\n");
        $structure = $decode->decode($params);
        print_r($structure);

        $decode_2 = new Mail_mimeDecode($structure->body, "\n");
        $structure_2 = $decode_2->decode($params);
        print_r($structure_2);

        die;

        /*            $this->_include_bodies = isset($params['include_bodies']) ?
          $params['include_bodies'] : false;
          $this->_decode_bodies  = isset($params['decode_bodies']) ?
          $params['decode_bodies']  : false;
          $this->_decode_headers = isset($params['decode_headers']) ?
          $params['decode_headers'] : false;
          $this->_rfc822_bodies  = isset($params['rfc_822bodies']) ?
          $params['rfc_822bodies']  : false;
         */

        // destination folder exists?
        $mailbox_idnr = $this->get_mail_box_id($folder);
        if (!$mailbox_idnr) {
            return FALSE;
        }

        $response = $this->store_message($mailbox_idnr, $message);

        console('store_message response:');
        console($response);

        return $response;
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

            if (!$this->dbmail->query($query) || $this->increment_message_seq($message_uid)) {
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
     * @param mixed  $uids  Message UIDs as array or comma-separated string, or '*'
     * @param string $to    Target folder
     * @param string $from  Source folder
     *
     * @return boolean True on success, False on error
     */
    public function copy_message($uids, $to, $from = null) {

        // format supplied message UIDs list
        $message_uids = $this->list_message_UIDs($uids, $from);
        if (!$message_uids) {
            return FALSE;
        }

        // destination folder exists?
        $to_mailbox_idnr = $this->get_mail_box_id($to);
        if (!$to_mailbox_idnr) {
            return FALSE;
        }

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        // loop messages
        foreach ($message_uids as $message_uid) {

            // extract message details
            $query = " SELECT * "
                    . " FROM dbmail_messages "
                    . " WHERE message_idnr = {$this->dbmail->escape($message_uid)} ";

            $res = $this->dbmail->query($query);

            if ($this->dbmail->num_rows($res) == 0) {
                // Record not found
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }

            $row = $this->dbmail->fetch_assoc($res);

            if ($row['mailbox_idnr'] == $to_mailbox_idnr) {
                // source folder equal to destination folder, do nothing!
                continue;
            }

            // copy selected message to target folder
            $query = " INSERT INTO dbmail_messages "
                    . " ( "
                    . "      mailbox_idnr, "
                    . "      physmessage_id, "
                    . "      seen_flag, "
                    . "      answered_flag, "
                    . "      deleted_flag, "
                    . "      flagged_flag, "
                    . "      recent_flag, "
                    . "      draft_flag, "
                    . "      unique_id, "
                    . "      status, "
                    . "      seq"
                    . " ) "
                    . " VALUES "
                    . " ( "
                    . "      {$this->dbmail->escape($to_mailbox_idnr)}, "
                    . "      {$this->dbmail->escape($row['physmessage_id'])}, "
                    . "      0, "
                    . "      0, "
                    . "      0, "
                    . "      0, "
                    . "      0, "
                    . "      0, "
                    . "      '{$this->dbmail->escape($row['unique_id'])}', "
                    . "      0, "
                    . "      0 "
                    . " ) ";

            if (!$this->dbmail->query($query)) {
                // Error while executing query
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
     * @param mixed  $uids    Message UIDs as array or comma-separated string, or '*'
     * @param string $folder  Source folder
     *
     * @return boolean True on success, False on error
     */
    public function delete_message($uids, $folder = null) {

        if (!strlen($folder)) {
            $folder = $this->folder;
        }

        // format supplied message UIDs list
        $message_uids = $this->list_message_UIDs($uids, $folder);
        if (!$message_uids) {
            return FALSE;
        }

        // start transaction
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        foreach ($message_uids as $message_uid) {

            $query = "DELETE FROM dbmail_messages "
                    . " WHERE message_idnr = {$this->dbmail->escape($message_uid)} ";

            if (!$this->dbmail->query($query)) {
                // rollbalk transaction
                $this->dbmail->rollbackTransaction();
                return FALSE;
            }
        }

        return ($this->dbmail->endTransaction() ? TRUE : FALSE);
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
        // TO DO!!!!!!!
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

                // rename sub-folder and increment 'seq' flag
                if (!$this->dbmail->query($query) || !$this->increment_mailbox_seq($sub_folder_idnr)) {
                    // rollbalk transaction
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }
            }
        }

        // rename target folder and increment 'seq' flag
        $query = "UPDATE dbmail_mailboxes "
                . " set name = '{$this->dbmail->escape($new_name)}' "
                . " WHERE mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

        if (!$this->dbmail->query($query) || !$this->increment_mailbox_seq($mailbox_idnr)) {
            // rollbalk transaction
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

                // rename sub folder
                $query = "DELETE FROM dbmail_mailboxes "
                        . " WHERE mailbox_idnr = {$this->dbmail->escape($sub_folder_idnr)} ";

                if (!$this->dbmail->query($query)) {
                    // rollbalk transaction
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }
            }
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
        // TO DO!!!!!
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

        $mailbox_idnr = $this->get_mail_box_id($folder);

        $query = " SELECT seen_flag, answered_flag, deleted_flag, flagged_flag, recent_flag, draft_flag, no_inferiors, no_select"
                . " FROM dbmail_mailboxes "
                . " WHERE mailbox_idnr = {$this->dbmail->escape($mailbox_idnr)} ";

        $res = $this->dbmail->query($query);
        $row = $this->dbmail->fetch_assoc($res);

        $result = array();

        if (!empty($row)) {

            $result = array(
                'seen' => $row['seen_flag'],
                'answered' => $row['answered_flag'],
                'deleted' => $row['deleted_flag'],
                'flagged' => $row['flagged_flag'],
                'recent' => $row['recent_flag'],
                'draft' => $row['draft_flag'],
                '\\Noselect' => $row['no_select']
            );
        }


        return $result;
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
        // TO DO!!!!!
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
        // TO DO!!!!
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

    protected function get_headers($physmessage_id) {

        $headers = array();

        $query = " SELECT dbmail_headername.headername, dbmail_headervalue.headervalue "
                . " FROM dbmail_header "
                . " INNER JOIN dbmail_headername ON dbmail_header.headername_id = dbmail_headername.id "
                . " INNER JOIN dbmail_headervalue ON dbmail_header.headervalue_id = dbmail_headervalue.id "
                . " WHERE dbmail_header.physmessage_id = {$this->dbmail->escape($physmessage_id)} ";

        $res = $this->dbmail->query($query);

        while ($row = $this->dbmail->fetch_assoc($res)) {

            $headername = $row['headername'];
            $headervalue = $row['headervalue'];

            $headers[$headername] = $headervalue;
        }

        return $headers;
    }

    /**
     * Retrive delimiter for supplied header name
     */
    protected function get_header_delimiter($token) {

        $match = array('boundary', 'filename', 'x-unix-mode', 'name', 'charset', 'format', 'size');

        $delimiter = ":";
        foreach ($match as $item) {
            if (strtoupper(substr($token, 0, strlen($item))) == strtoupper($item)) {
                $delimiter = "=";
                break;
            }
        }

        return $delimiter;
    }

    /**
     * Search for a specific header name within supplied headers list
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
     * Return header id
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
     * Return header id
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
     * Return multi-part data for a specific message
     *
     * @param rcube_message_header  $headers
     * @param int                   $id       Message UID
     * @param string                $ctype    Message content type
     *
     * @return rcube_message_header Message headers
     */
    private function get_message_multipart_data($headers, $uid, $ctype = '') {

        list($ctype_primary, $ctype_secondary) = explode('/', $ctype);

        $query = "SELECT dbmail_partlists.part_key, dbmail_partlists.is_header, dbmail_mimeparts.data, dbmail_mimeparts.size"
                . " FROM dbmail_messages "
                . " INNER JOIN dbmail_physmessage ON dbmail_messages.physmessage_id = dbmail_physmessage.id "
                . " INNER JOIN dbmail_partlists ON dbmail_physmessage.id = dbmail_partlists.physmessage_id "
                . " INNER JOIN dbmail_mimeparts ON dbmail_partlists.part_id = dbmail_mimeparts.id "
                . " WHERE dbmail_messages.message_idnr = {$this->dbmail->escape($uid)} "
                . " AND part_key > 1 "
                . " ORDER BY part_key ASC, part_order ASC, is_header DESC ";

        $part_list_result = $this->dbmail->query($query);

        // group part-list
        $multi_part_items = array();
        while ($part = $this->dbmail->fetch_assoc($part_list_result)) {

            $part_key = $part['part_key'];
            $is_header = $part['is_header'];
            $data = $part['data'];
            $size = $part['size'];

            if ($is_header == 1) {

                $part_ctype = $this->get_header_value($data, 'Content-Type');

                list($part_ctype_primary, $part_ctype_secondary) = explode('/', $part_ctype);

                $multi_part_items[$part_key]['ctype'] = $part_ctype;
                $multi_part_items[$part_key]['ctype_primary'] = $part_ctype_primary;
                $multi_part_items[$part_key]['ctype_secondary'] = $part_ctype_secondary;
                $multi_part_items[$part_key]['encoding'] = $this->get_header_value($data, 'Content-Transfer-Encoding');
                $multi_part_items[$part_key]['charset'] = $this->get_header_value($data, 'charset');
                $multi_part_items[$part_key]['headers'] = explode("\n", $data);
            } else {
                $multi_part_items[$part_key]['data'] .= $data;
                $multi_part_items[$part_key]['size'] = (array_key_exists('size', $multi_part_items[$part_key]) ? $multi_part_items[$part_key]['size'] + $size : $size);
            }
        }

        // set base rcube_message_part
        $structure = new rcube_message_part();
        $structure->mime_id = 0;
        $structure->ctype_primary = $ctype_primary;
        $structure->ctype_secondary = $ctype_secondary;
        $structure->mimetype = $ctype;
        $structure->filename = '';
        $structure->encoding = '8bit';
        $structure->charset = '';
        $structure->size = 0;
        $structure->headers = array();
        $structure->d_parameters = array();
        $structure->ctype_parameters = array();
        $structure->parts = array();

        // init $bodystructure array
        $bodystructure = array();

        // fetch multi-part items
        $sequence = 1;
        foreach ($multi_part_items as $multi_part_item) {

            $part_item = new rcube_message_part();
            $part_item->mime_id = $sequence;
            $part_item->ctype_primary = $multi_part_item['ctype_primary'];
            $part_item->ctype_secondary = $multi_part_item['ctype_secondary'];
            $part_item->mimetype = $multi_part_item['ctype'];
            $part_item->disposition = '';
            $part_item->filename = '';
            $part_item->encoding = $multi_part_item['encoding'];
            $part_item->charset = $multi_part_item['charset'];
            $part_item->size = $multi_part_item['size'];
            $part_item->headers = array('content-transfer-encoding' => 'quoted-printable');
            $part_item->d_parameters = array();
            $part_item->ctype_parameters = array('charset' => 'ISO-8859-1');

            $structure->parts[] = $part_item;

            $bodystructure[] = array(
                $multi_part_item['ctype_primary'],
                $multi_part_item['ctype_secondary'],
                array(
                    'charset',
                    $multi_part_item['charset']
                ),
                NULL,
                NULL,
                $multi_part_item['encoding'],
                $multi_part_item['size'],
                17, // ??????
                NULL,
                NULL,
                NULL,
                NULL
            );

            $sequence++;
        }

        $bodystructure[] = array(
            $ctype_secondary,
            array(
                'boundary',
                'B_3507634464_6506483'
            ),
            NULL,
            NULL,
            NULL
        );

        $headers->structure = $structure;
        $headers->bodystructure = $bodystructure;

        return $headers;
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
    protected function list_message_UIDs($uids, $folder = '') {

        $message_UIDs = array();

        if (is_string($uids) && $uids == '*' && strlen($folder) > 0) {

            // full folder request
            $mailbox_idnr = $this->get_mail_box_id($folder);

            $query = "SELECT message_idnr "
                    . " FROM dbmail_messages "
                    . " WHERE mailbox_idnr = '{$this->dbmail->escape($mailbox_idnr)}'";

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
                    /*
                      $formatted_filter[] = " "
                      . " ( "
                      . "     filter_dbmail_header.headername_id = '{$this->dbmail->escape($header_id)}' "
                      . "     AND filter_dbmail_headervalue.headervalue <> 1 "
                      . "     AND filter_dbmail_headervalue.headervalue <> 2 "
                      . "     AND filter_dbmail_headervalue.headervalue <> 3 "
                      . "     AND filter_dbmail_headervalue.headervalue <> 5 "
                      . " ) ";
                     */
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
                        . " INNER JOIN dbmail_header AS sort_dbmail_header ON dbmail_physmessage.id = sort_dbmail_header.physmessage_id AND sort_dbmail_header.headername_id = {$this->dbmail->escape($header_id)} "
                        . " INNER JOIN dbmail_headervalue AS sort_dbmail_headervalue ON sort_dbmail_header.headervalue_id = sort_dbmail_headervalue.id ";

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

            $message_headers = $this->get_headers($physmessage_id);

            $rcmh = new rcube_message_header();

            $rcmh->id = $msg_index;
            $rcmh->uid = $message_idnr;
            $rcmh->folder = $folder;
            $rcmh->subject = (is_array($message_headers) && array_key_exists('subject', $message_headers) ? $message_headers['subject'] : '');
            $rcmh->from = (is_array($message_headers) && array_key_exists('from', $message_headers) ? $message_headers['from'] : '');
            $rcmh->to = (is_array($message_headers) && array_key_exists('to', $message_headers) ? $message_headers['to'] : '');
            $rcmh->replyto = (is_array($message_headers) && array_key_exists('return-path', $message_headers) ? $message_headers['return-path'] : '');
            $rcmh->in_reply_to = (is_array($message_headers) && array_key_exists('return-path', $message_headers) ? $message_headers['return-path'] : '');
            $rcmh->date = (is_array($message_headers) && array_key_exists('date', $message_headers) ? $message_headers['date'] : '');
            $rcmh->internaldate = (is_array($message_headers) && array_key_exists('date', $message_headers) ? $message_headers['date'] : '');
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

    private function store_message($mailbox_idnr, $content) {



        echo 'TO DO!!!!!!!';
        die;

        /*
         * TO DO!!!!!!!!!!
         * 
         * fix values for:
         * - dbmail_physmessage.messagesize
         * - dbmail_physmessage.rfcsize
         * - dbmail_partlists.part_depth
         * - dbmail_partlists.part_order
         * - dbmail_mimeparts.hash
         * - dbmail_headervalue.hash
         * - dbmail_headervalue.sortfield
         * - dbmail_headervalue.datefield
         */


        console('------------------------------------------------------');
        console('INPUT PARAMETERS');
        console($mailbox_idnr);
        console($content);
        console('------------------------------------------------------');
        die;


        /*
         *  explode suplied content on new line
         */
        $exploded_content = explode("\n", $content);

        /*
         * resolve content-type to handle multi-part messages 
         * (stop on first content-type header occurrence)
         */
        $ctype = FALSE;
        $is_multipart = FALSE;

        foreach ($exploded_content as $row) {
            $ctype = $this->get_header_value($row, 'Content-Type');
            if ($ctype && strtoupper(substr($ctype, 0, 9)) == 'MULTIPART') {
                // multipart content-type found
                $is_multipart = TRUE;
                break;
            } elseif ($ctype) {
                // text/plain content-type found
                $is_multipart = FALSE;
                break;
            }
        }

        /*
         * force Content-Type 'text/plain' if not found (eg. empty content message)
         */
        $ctype = ($ctype ? $ctype : 'text/plain');

        /*
         * on multi-part messages retrive supplied boundary
         */
        $boundary = FALSE;
        $inner_boundary = FALSE;
        $ending_boundary = FALSE;
        if ($is_multipart) {
            foreach ($exploded_content as $row) {
                $boundary = $this->get_header_value($row, 'boundary');
                if ($boundary) {
                    // ok - boundary found
                    $inner_boundary = "--{$boundary}";
                    $ending_boundary = "--{$boundary}--";
                    break;
                }
            }

            if (!$boundary) {
                // error - boundary not found
                return FALSE;
            }
        }

        /*
         * prepare message parts container
         */
        $message_parts = array();
        $part_index = 0;
        $is_header = TRUE;
        foreach ($exploded_content as &$row) {

            $row = trim($row);

            // header / content are separate by an empty row
            if (strlen($row) == 0) {
                // toggle target and skip to next row
                $is_header = ($is_header ? FALSE : TRUE);
                continue;
            }

            // boundary management
            if ($row == $inner_boundary) {
                /*
                 *  $inner_boundary found
                 * - increment part index and skip to next row
                 * - set $is_header flag
                 */
                $part_index++;
                $is_header = TRUE;
                continue;
            } elseif ($row == $ending_boundary) {
                // $ending_boundary found - exit loop
                break;
            }

            $target = ($is_header ? 'headers' : 'content');

            if (!array_key_exists($part_index, $message_parts) || !array_key_exists($target, $message_parts[$part_index])) {
                $message_parts[$part_index][$target] = $row;
            } else {
                $message_parts[$part_index][$target] .= PHP_EOL . $row;
            }
        }

        /*
         * start transaction
         */
        if (!$this->dbmail->startTransaction()) {
            return FALSE;
        }

        /*
         * insert dbmail_physmessage record
         */
        $query = "INSERT INTO dbmail_physmessage "
                . " ("
                . "    messagesize, "
                . "    rfcsize, "
                . "    internal_date"
                . " ) "
                . " VALUES "
                . " ( "
                . "    '{$this->dbmail->escape(strlen($content))}', "
                . "    '{$this->dbmail->escape(strlen($content))}', "
                . "    '{$this->dbmail->escape(date("Y-m-d H:i:s"))}' "
                . ")";

        if (!$this->dbmail->query($query)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        /*
         * retrive physmessage_id
         */
        $physmessage_id = $this->dbmail->insert_id('dbmail_physmessage');
        if (!$physmessage_id) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        /*
         * insert dbmail_messages record
         */
        $query = "INSERT INTO dbmail_messages "
                . " ("
                . "    mailbox_idnr,"
                . "    physmessage_id, "
                . "    seen_flag, "
                . "    answered_flag,"
                . "    deleted_flag, "
                . "    flagged_flag, "
                . "    recent_flag, "
                . "    draft_flag, "
                . "    unique_id, "
                . "    status, "
                . "    seq "
                . " ) "
                . " VALUES "
                . " ( "
                . "    '{$this->dbmail->escape($mailbox_idnr)}', "
                . "    '{$this->dbmail->escape($physmessage_id)}', "
                . "    0, "
                . "    0, "
                . "    0, "
                . "    0, "
                . "    0, "
                . "    0, "
                . "    '{$this->dbmail->escape($this->create_message_unique_id())}', "
                . "    0, "
                . "    0 "
                . " ) ";

        if (!$this->dbmail->query($query)) {
            $this->dbmail->rollbackTransaction();
            return FALSE;
        }

        /*
         * insert dbmail_header record 
         * (header deduplication aware!!!)
         */
        if (!array_key_exists(0, $message_parts) || !array_key_exists('headers', $message_parts[0])) {
            // error - no headers found
            return FALSE;
        }

        $headers = $message_parts[0]['headers'];
        $exploded_headers = explode("\n", $headers);
        foreach ($exploded_headers as $row) {

            // header could contain more than one parameter, split row on semicolon (if present)
            $exploded_row = explode(';', $row);

            foreach ($exploded_row as &$header) {

                $header = trim($header);

                if (strlen($header) > 0) {

                    // split by header name / header value
                    $delimiter = $this->get_header_delimiter($header);

                    $exploded_header = explode($delimiter, $header, 2);

                    if (count($exploded_header) != 2) {
                        // error 
                        return FALSE;
                    }

                    $header_name = $exploded_header[0];
                    $header_value = $exploded_header[1];

                    $header_name_id = $this->get_header_id_by_header_name($header_name);
                    if (!$header_name_id) {

                        // new header name found - insert record
                        $query = "INSERT INTO dbmail_headername "
                                . " ( "
                                . "     headername "
                                . " ) "
                                . " VALUES "
                                . " ( "
                                . "     '{$this->dbmail->escape($header_name)}' "
                                . " ) ";

                        if (!$this->dbmail->query($query)) {
                            $this->dbmail->rollbackTransaction();
                            return FALSE;
                        }

                        // retrive header name id
                        $header_name_id = $this->dbmail->insert_id('dbmail_headername');
                        if (!$header_name_id) {
                            $this->dbmail->rollbackTransaction();
                            return FALSE;
                        }
                    }

                    $header_value_id = $this->get_header_value_id_by_header_value($header_value);
                    if (!$header_value_id) {

                        // new header value found - insert record
                        $query = "INSERT INTO dbmail_headervalue "
                                . " ( "
                                . "     hash, "
                                . "     headervalue, "
                                . "     sortfield, "
                                . "     datefield "
                                . " )"
                                . " VALUES"
                                . " ( "
                                . "     '{$this->dbmail->escape(md5($header_value))}', "
                                . "     '{$this->dbmail->escape($header_value)}', "
                                . "     '{$this->dbmail->escape($header_value)}', "
                                . "     NULL "
                                . " ) ";

                        if (!$this->dbmail->query($query)) {
                            $this->dbmail->rollbackTransaction();
                            return FALSE;
                        }

                        // retrive header value id
                        $header_value_id = $this->dbmail->insert_id('dbmail_headervalue');
                        if (!$header_value_id) {
                            $this->dbmail->rollbackTransaction();
                            return FALSE;
                        }
                    }

                    // headers table record exists?
                    $query = " SELECT physmessage_id "
                            . " FROM dbmail_header "
                            . " WHERE physmessage_id = '{$this->dbmail->escape($physmessage_id)}' "
                            . " AND headername_id = '{$this->dbmail->escape($header_name_id)}' "
                            . " AND headervalue_id = '{$this->dbmail->escape($header_value_id)}' ";

                    $res = $this->dbmail->query($query);

                    if ($this->dbmail->num_rows($res) == 0) {

                        // add main headers table record
                        $query = "INSERT INTO dbmail_header "
                                . " ( "
                                . "     physmessage_id, "
                                . "     headername_id, "
                                . "     headervalue_id "
                                . " "
                                . " )"
                                . " VALUES "
                                . " ( "
                                . "     '{$this->dbmail->escape($physmessage_id)}', "
                                . "     '{$this->dbmail->escape($header_name_id)}', "
                                . "     '{$this->dbmail->escape($header_value_id)}' "
                                . " )";

                        if (!$this->dbmail->query($query)) {
                            $this->dbmail->rollbackTransaction();
                            return FALSE;
                        }
                    }
                }
            }
        }

        foreach ($message_parts as $index => $part_data) {

            $part_key = ($index + 1);

            // headers management
            if (array_key_exists('headers', $part_data) && strlen($part_data['headers']) > 0) {

                $query = "INSERT INTO dbmail_mimeparts "
                        . " ( "
                        . "     hash, "
                        . "     data, "
                        . "     size "
                        . " ) "
                        . " VALUES "
                        . " ( "
                        . "     '{$this->dbmail->escape(md5($part_data['headers']))}', "
                        . "     '{$this->dbmail->escape($part_data['headers'])}', "
                        . "     '{$this->dbmail->escape(strlen($part_data['headers']))}' "
                        . " ) ";

                if (!$this->dbmail->query($query)) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }

                // retrive part list header id
                $header_part_list_id = $this->dbmail->insert_id('dbmail_mimeparts');
                if (!$header_part_list_id) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }

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
                        . "    '1', "
                        . "    '{$this->dbmail->escape($part_key)}', "
                        . "    '0', "
                        . "    '0', "
                        . "    '{$this->dbmail->escape($header_part_list_id)}' "
                        . " ) ";

                if (!$this->dbmail->query($query)) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }
            }

            // content management
            if (array_key_exists('content', $part_data) && count($part_data['content']) > 0) {

                $query = "INSERT INTO dbmail_mimeparts "
                        . " ( "
                        . "     hash, "
                        . "     data, "
                        . "     size "
                        . " ) "
                        . " VALUES "
                        . " ( "
                        . "     '{$this->dbmail->escape(md5($part_data['content']))}', "
                        . "     '{$this->dbmail->escape($part_data['content'])}', "
                        . "     '{$this->dbmail->escape(strlen($part_data['content']))}' "
                        . " ) ";

                if (!$this->dbmail->query($query)) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }

                // retrive part list header id
                $comntent_part_list_id = $this->dbmail->insert_id('dbmail_mimeparts');
                if (!$comntent_part_list_id) {
                    $this->dbmail->rollbackTransaction();
                    return FALSE;
                }

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
                        . "    '0', "
                        . "    '{$this->dbmail->escape($part_key)}', "
                        . "    '0', "
                        . "    '0', "
                        . "    '{$this->dbmail->escape($comntent_part_list_id)}' "
                        . " ) ";

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
        $finalized = false;
        $is_header = true;
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

            $prevdepth = $depth;
            $prev_header = $is_header;

            $depth = $mimePart['part_depth'];
            $is_header = $mimePart['is_header'];
            $blob = $mimePart['data'];

            if ($is_header) {
                $prev_boundary = $got_boundary;
                $prev_is_message = $is_message;

                $is_message = preg_match('~content-type:\s+message/rfc822\b~i', $blob);
            }

            $got_boundary = false;

            $matches = array();
            if ($is_header && preg_match('~^content-type:\s+.*;(\r?\n\s.*)*\s+boundary="?([a-z0-9\'()+_,-./:=\?]*)~mi', $blob, $matches)) {
                list(,, $boundary) = $matches;
                $got_boundary = true;
                $blist[$depth] = $boundary;
            }

            while (($prevdepth > 0) && ($prevdepth - 1 >= $depth) && $blist[$prevdepth - 1]) {
                $body .= $newline . "--" . $blist[$prevdepth - 1] . "--" . $newline;
                unset($blist[$prevdepth - 1]);
                $prevdepth--;
                $finalized = true;
            }


            if (($depth > 0) && (!empty($blist[$depth - 1]))) {
                $boundary = $blist[$depth - 1];
            }

            if ($is_header && (!$prev_header || $prev_boundary || ($prev_header && $depth > 0 && !$prev_is_message))) {
                if ($prevdepth > 0) {
                    $body .= $newline;
                }
                $body .= "--" . $boundary . $newline;
            }

            if (!$is_header && $prev_header) {
                $body .= $newline;
            }

            if ($is_header && $depth == 0) {
                $header .= $blob;
            } else {
                $body .= $blob;
            }

            $index++;
        }

        if ($index > 2 && $boundary && !$finalized) {
            $body .= $newline . "--" . $boundary . "--" . $newline;
        }

        $response = new stdClass();
        $response->header = $header;
        $response->body = $body;

        return $response;
    }

}
