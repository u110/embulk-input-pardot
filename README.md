# Pardot input plugin for Embulk

Now only 'Querying Visitor Activities' is supported.
- https://developer.pardot.com/kb/api-version-4/visitor-activities/

## Overview

* **Plugin type**: input
* **Resume supported**: yes
* **Cleanup supported**: yes
* **Guess supported**: no

## Configuration

- **user_name**: pardot/salesforce email (string, required)
- **password**: pardot/salesforce password (string, required)
- **user_key**: pardot user-key (string, default: `null`)
- **app_client_id**: salesforce app-client-id (string, default: `null`)
- **app_client_secret**: salesforce app-client-secret (string, default: `null`)
- **business_unit_id**: salesforce business-unit-id (string, default: `null`)
- **created_after**: Selects visitor activities created after the specified time. If a <custom_time> is used, ensure that the specified date is formatted using GNU Date Input Syntax.
  - today, yesterday, last_7_days, this_month, last_month, <custom_time>
- **created_before**: Selects visitor activities created before the specified time. If a <custom_time> is used, ensure that the specified date is formatted using GNU Date Input Syntax.
  - today, yesterday, last_7_days, this_month, last_month, <custom_time>
- **fetch_row_limit**: Specifies the number of results(per 1 request) to be returned. Default value: 200. Note: This number cannot be larger than 200.

see API document
- https://developer.pardot.com/kb/api-version-4/visitor-activities/

## Example

### With salesforce client ID / secret

```yaml
in:
  type: pardot
  user_name: dummy@example.com
  password: password**
  app_client_id: app-client-id**
  app_client_secret: app-client-secret**
  business_unit_id: business-unit-id**
  created_after: 2020-12-01
  created_before: 2020-12-02
```

### With pardot user-key (will be deprecated in spring 2021)

- see https://help.salesforce.com/articleView?id=000353746&type=1&mode=1&language=en_US

```yaml
in:
  type: pardot
  user_name: dummy@example.com
  password: password**
  user_key: user-key**
  created_after: 2020-12-01
  created_before: 2020-12-02
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
