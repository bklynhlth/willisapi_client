# willisapi_client
A Python client for willisAPI

```
import willisapi_client as wc

key, expire_in = wc.login(username='dishant.sethi@brooklyn.health', password="Test@123")
wc.create_user(key, 'dishantsethi14@gmail.com', 'CompanyName')
wc.upload(key, '/Users/dishantsethi/Development/api-client/willisapi_client/metadata.csv')
```
