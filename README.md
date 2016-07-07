# roundcube-dbmail
Repository for the DBMAIL Storage plugin for Roundcube

---
## Introduction

rcube_dmail is a _storage plugin_ for roundcube webmail (http://roundcube.net), intended to directly hook roundcube into dbmail (http://dbmail.org/) database.

Current release of the plugin implement all the basic functionalities of RoundCube.

Currently everything work but thread-view.

The plugin is currently only tested with **MySQL**, but plans include support for postgre and other.

Any help is greatly welcome :-)

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

// Enable DBmail Caching
$config["dbmail_cache"] = "db";      // Generic cache switch
$config["messages_cache"] = TRUE;    // Cache for messages. We don't use it
$config["dbmail_cache_ttl"] = "10d"; // Cache default expire value
```

---
## Sponsorship

This project is currently sponsored by Schema31 S.p.A. (http://www.schema31.it/)
