# willisapi_client
A Python client for willisAPI

```
import willisapi_client as wc

key, expire_in = wc.login(username='', password="")
message = wc.create_user(key, 'admin_username', 'CompanyName')
dataframe = wc.upload(key, 'METADATA_CSV_PATH')
json = wc.download(key, 'project_name')
```

## Update env while testing/development
- path/to/willisapi_client/willisapi_client.py
    - self.env = 'dev'