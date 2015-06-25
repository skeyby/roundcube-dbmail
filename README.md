# roundcube-dbmail
Repository for the DBMAIL Storage plugin for Roundcube

---
## Introduction

rcube_dmail is a _storage plugin_ for roundcube webmail (http://roundcube.net), intended to directly hook roundcube into dbmail (http://dbmail.org/) database.

the plugin is a pretty rough proof of concept right now, and is not intended (yet) for real usage. it's currently functionalities are:
* Login
* Folder listing
* Messages listing
* Message retrival

Missing functionalities, currently under development, are:
* Searching functions
* Message writing

The plugin is currently only tested with **MySQL**, but plans include support for postgre and other.

any help is greatly welcome :-)

---
## Installation

TO ENABLE 'rcube_dbmail' PLUGIN:
* drop rcube_dbmail.php to 'program/lib/Roundcube'
* add the following lines to roundcube/config/config.inc.php:

```php
$config['storage_driver'] = 'dbmail';
$config['dbmail_dsn'] = 'mysql://user:pass@host/db'; # dsn connection string
$config['dbmail_hash'] = 'sha1'; # hashing method to use, must coincide with dbmail.conf - sha1, md5, sha256, sha512, whirlpool. sha1 is the default
$config['dbmail_fixed_headername_cache'] = FALSE; # add new headernames (if not exists) in 'dbmail_headername' when saving messages
```

!!! IMPORTANT !!!
Use the official PEAR Mail_mimeDecode library, changing following line in 'composer.json'
- change:  "pear/mail_mime-decode": ">=1.5.5",
- to:      "pear-pear.php.net/Mail_mimeDecode": ">=1.5.5",

---
## Sponsorship

This project is currently sponsored by Schema31 S.p.A. (http://www.schema31.it/)
