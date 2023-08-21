<!-- Output copied to clipboard! -->

<!-----

Yay, no errors, warnings, or alerts!

Conversion time: 0.36 seconds.


Using this Markdown file:

1. Paste this output into your source file.
2. See the notes and action items below regarding this conversion run.
3. Check the rendered output (headings, lists, code blocks, tables) for proper
   formatting and use a linkchecker before you publish this page.

Conversion notes:

* Docs to Markdown version 1.0β34
* Mon Aug 21 2023 13:41:56 GMT-0700 (PDT)
* Source doc: README.md
----->



### WillisAPI Client

WillisAPI Client is the python interface for Brooklyn Health’s WillisAPI.

Official documentation for WillisAPI Client can be found in the [Github Wiki](www.github.com/bklynhlth/willisapi_client/wiki).

To learn more about Brooklyn Health or WillisAPI, visit [brooklyn.health](https://www.brooklyn.health) or [getintouch@brooklyn.health](mailto:getintouch@brooklyn.health).


#### Installation


```
pip install willisapi_client
```


**Login**

Before you log in, make sure you have an account with Brooklyn Health.


```
import willisapi_client as willisapi
key, expiration = willisapi.login(username, password)
```


**Upload**


```
summary = willisapi.upload(key, 'data.csv')
```


For more information on how to organize the `data.csv`, visit the [Github Wiki](http://www.github.com/bklynhlth/willisapi_client/wiki).


#### Download


```
measures = willisapi.download(key, 'project_name')
```


If you run into trouble while using the client, please raise it in the [Issues](www.github.com/bklynhlth/willisapi_client/issues) tab. 

***

Brooklyn Health is a small team of clinicians, scientists, and engineers based in Brooklyn, NY. 

We develop and maintain [OpenWillis](www.github.com/bklynhlth/openwillis), an open source python library for digital health measurement. 
