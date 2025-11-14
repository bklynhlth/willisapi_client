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

WillisAPI Client is the Python interface for Brooklyn Health’s WillisAPI.

Official documentation for WillisAPI Client can be found in the [Github Wiki](http://www.github.com/bklynhlth/willisapi_client/wiki).

To learn more about Brooklyn Health or WillisAPI, visit [brooklyn.health](https://www.brooklyn.health) or [getintouch@brooklyn.health](mailto:getintouch@brooklyn.health).

---

#### Installation


```bash
pip install willisapi_client
```

---

#### Getting a Personal Access Token (PAT)
1. Log in to the Brooklyn Health web application.
2. Go to your profile section.
3. Navigate to Personal Access Token (PAT) and copy it for use in the client.

---

#### Usage

**Upload**
To upload a CSV file:

```python
summary = willisapi.upload(key, '/path/to/data.csv')
```

How to Call the Function
- key: Your PAT token.
- data.csv: Path to your CSV file.

How to Reupload

If you need to reupload the same or updated file, simply call the upload function again:

```python
summary = willisapi.upload(key, 'data.csv', force_uploade=True)
```
---
#### Understanding Returned DataFrame and Errors

- The upload function returns a summary DataFrame containing the results of your upload.
- Columns typically include status, error messages, and metadata for each row in your CSV.
- If there are errors, they will be listed in the DataFrame under an error or message column.
- Review the DataFrame to identify and resolve any issues before reuploading.

---

#### CSV Validation Details

Before uploading, the client validates your CSV for:

- **Required Columns**: Must include study_id, site_id, rater_email, participant_id, visit_name, visit_order, coa_name, coa_item_number, coa_item_value, file_path, time_collected.
- **Optional Columns**: age, sex, race, language.
- **Valid COA Name**: Only these values are allowed: MADRS, YMRS, PHQ-9, GAD-7.
- **Valid Audio File**: The file path in each row must exist and be accessible.
- **Valid Email**: rater_email must be a valid email.
- **Valid Data Types**: visit_order, age, coa_item_number, and coa_item_value must be numeric.
- **Language**: If present, must be in the allowed language choices.

If any validation fails, errors are collected and returned for review before upload proceeds.

---

For more information on how to organize the `data.csv`, visit the [Github Wiki](http://www.github.com/bklynhlth/willisapi_client/wiki).


If you run into trouble while using the client, please raise it in the [Issues](http://www.github.com/bklynhlth/willisapi_client/issues) tab. 

***

Brooklyn Health is a small team of clinicians, scientists, and engineers based in Brooklyn, NY. 

We develop and maintain [OpenWillis](http://www.github.com/bklynhlth/openwillis), an open source python library for digital health measurement. 
