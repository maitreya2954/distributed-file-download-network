# Server

Install all the required packages. 
#### One time setup
```
python
```
The above command will initiate a python bash in your command line where you can use further lines of code to create your data table according to your model class in your database. 
```
from app import db
```
```
db.create_all()
```
Now to create migrations we run the following commands one after the other.
```
flask db init
```
```
flask db migrate -m "Initial migration"
```
```
flask db upgrade
```
#### Starting a server

```
python -m flask run --host="0.0.0.0" --port=9999 --debug
```
