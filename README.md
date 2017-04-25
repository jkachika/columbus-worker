# Columbus Worker Library

A library for distributed execution of workflows submitted through Columbus workflow engine. The library methods are 
intended to be used in the code composed for Components and Combiners of the [Columbus platform](https://github.com/jkachika/columbus).
 
## Worker API
Refer to the API for the methods that can be used in the code, [here](https://jkachika.github.io/colorker/index.html).

## Requirements
 * Linux based OS
 * Python 2.7
 
## Output Types
Columbus supports the following output types - CSV List, Feature, Feature Collection, Multi Collection, and Blob. 
All output data is transferred as is to subsequent elements in the workflow.

### CSV List
Data is represented a list of python dictionaries.
```python
[{'car_speed': 10.54, 'ch4': 3.56, 'locality':'Fort Collins'},
 {'car_speed': 11.10, 'ch4': 6.5, 'locality': 'Denver'} ...]
```

### Feature
Data is represented as geojson for Feature and must be an instance of [geojson.Feature](https://pypi.python.org/pypi/geojson/#feature). "properties" must be a simple dictionary with key as string and value as any of the primitive types however a Feature can include any picklable value as part of its dictionary apart from "geometry", "properties" and "type".
```python
>>> from geojson import Feature, Point
>>> my_point = Point((-3.68, 40.41))
>>> Feature(geometry=my_point) # doctest: +ELLIPSIS
{"geometry": {"coordinates": [-3.68..., 40.4...], "type": "Point"}, 
 "properties": {}, "type": "Feature"}
```

### FeatureCollection
Data is represented as geojson for FeatureCollection and must be an instance of [geojson.FeatureCollection](https://pypi.python.org/pypi/geojson/#featurecollection). Must contain "columns" dictionary property as part of its dictionary and it should have the property names of the features in the FeatureCollection as its keys and the data type of those properties as values. A FeatureCollection can include any picklable value as part of its dictionary apart from "features", "columns" and "type".
```python
>>> from geojson import Feature, Point, FeatureCollection

>>> my_feature = Feature(geometry=Point((1.6432, -19.123)))
>>> my_feature["properties"]["temperature"] = 32.5
>>> my_other_feature = Feature(geometry=Point((-80.234, -22.532)))
>>> my_other_feature["properties"]["temperature"] = 20.7

>>> myftc = FeatureCollection([my_feature, my_other_feature])  # doctest: +ELLIPSIS
>>> myftc["columns"] = {"temperature" : "FLOAT"}
>>> print myftc
{"features": [{"geometry": {"coordinates": [1.643..., -19.12...], "type": "Point"}, 
               "properties": {"temperature": 32.5}, 
               "type": "Feature"
              }, 
              {"geometry": {"coordinates": [-80.23..., -22.53...], "type": "Point"}, 
               "properties": {"temperature": 20.7}, 
               "type": "Feature"
              }],
  "columns": {"temperature" : "FLOAT"},
  "type": "FeatureCollection"
}
```

### MultiCollection
Data is represented as a python List of [geojson.FeatureCollection](https://pypi.python.org/pypi/geojson/#featurecollection)s

### Blob
Data is represented as any pickable python object.

  
## Script Usage
Scripts should make use of the internal variables `__input__` to get the input data and assign the output to `__output__`
to make the data available to its dependents.

### Reading Data
Reading data for a root component
```python
csv_list = __input__
```

Reading data for a non-root component having `component-1` and `combiner-1` as its parents. 
`component-1` and `combiner-1` are id values of the parent component and combiner respectively
```python
parent1 = __input__["component-1"]
parent2 = __input__["combiner-1"]
```

Reading data for a combiner
```python
a_list = __input__["workflow"]
```

### Writing Data
To write data, build a structure of the chosen output type and assign it to `__output__`
```python
__output__ = csv_list
```

### Google Fusion Tables
To get the fusion table key in components whose parents are visualizers.
```python
>>> component_ftkey = __input__["ftkey"]["component-1"]
>>> combiner_ftkey = __input__["ftkey"]["combiner-1"]
>>> print component_ftkey
10tSob7imDONyigihnAamYK7kmidDz2l6H5b1qVSf
>>> print combiner_ftkey
1oMf16v9Iw4lmoOLKmjRB5hnZIXVVcWfK_rGHrtC7,1QsFzkZJtLkBeF0NkN_piGjdXl_JEnxnCk__LAgSK
```

> For MultiCollection output, a single string will have all fusion table keys separated by comma. In the above example, `combiner-1` is a visualizer that produces MultiCollection output and `component-1` is also a visualizer that produces a FeatureCollection as its output.

Reading fusion tables of a workflow in a combiner
```python
a_list = __input__["ftkey"] # list of fusion table identifiers
```

### Google Earth Engine
Columbus makes use of [Google Earth Engine](https://developers.google.com/earth-engine/) to do all the GIS computations. Below are a few 
examples to obtain the earth engine from Columbus, getting the Columbus compatible geojson for the Earth Engine's FeatureCollection and how to 
use fusion tables with Earth Engine.

Obtaining Google Earth Engine to do GIS computations.
```python
from colorker.security import CredentialManager

ee = CredentialManager.get_earth_engine()
```

Obtaining GeoJSON from Earth Engine FeatureCollection.
```python
from colorker.service.gee import get_geojson

ftc = ee.FeatureCollection('ft:1oMf16v9Iw4lmoOLKmjRB5hnZIXVVcWfK_rGHrtC7')
ftc_geojson = get_geojson(ftc)
```

Using Fusion Table with Google Earth Engine.
```python
ftkey = __input__['ftkey']['component-1']
# Loading data from fusion table
ftc = ee.FeatureCollection('ft:' + str(ftkey))
```

### Sending Email
Columbus allows you to send an email from with in your script so you can decide when to send an email 
and completely take control of how the email content should look - whether to use plain text or rich HTML.
```python
from colorker.service.email import send_mail

send_email(['abc@xyz.com', 'def@pqr.com'], 'Subject of the Message',
'Hi There! This is a plain text message body',
'<b>Hi There!</b><br/><p>This is a HTML message body</p>')
```