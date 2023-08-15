# willisapi_client
A Python client for willisAPI

```
import willisapi_client as wc

key, expire_in = wc.login(username='', password="")
wc.create_user(key, 'admin_username', 'CompanyName')
wc.upload(key, 'METADATA_CSV_PATH')
```
