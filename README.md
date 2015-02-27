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
* drop rcube_dbmail.php to '../program/lib/Roundcube'
* add the following lines to roundcube/config/config.inc.php:

```php
$config['storage_driver'] = 'dbmail';
$config['dbmail_dsn'] = 'mysql://user:pass@host/db'; # dsn connection string
```

---
## Sponsorship

This project is currently sponsored by Schema31 S.p.A. (http://www.schema31.it/)
