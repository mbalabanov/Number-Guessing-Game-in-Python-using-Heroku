"""
SmartNinja NoSQL is a simple ODM tool which helps you switch between four NoSQL database systems: TinyDB, Firestore,
Datastore, MongoDB and Cosmos DB (via MongoDB API).

TinyDB is used for localhost development. The advantage is that it saves you time configuring a Firestore or Cosmos
emulator on localhost.

When you deploy your web app to Google Cloud, Heroku or Azure, the ODM figures out the new environment (through env
variables) and switches the database accordingly.

Bear in mind that this is a simple ODM meant to be used at SmartNinja courses for learning purposes. So not all
features of these NoSQL databases are covered, only the basic ones.

Created by: Matej Ramuta
www.smartninja.org
December 2018
"""

import os
import logging

# check where the app is currently hosted (GAE-Google App Engine, Localhost or Azure App Service)
if os.environ.get("GAE_APPLICATION"):
    server_env = "gae"
    print("Platform: GAE")

    if os.getenv("GAE_DATABASE") == "datastore":
        # the "GAE_DATABASE" env var must be set in app.yaml
        from google.cloud import datastore
        print("Datastore selected.")
        gae_database = "datastore"
    else:
        import firebase_admin
        from firebase_admin import credentials
        from firebase_admin import firestore
        print("Firestore selected.")
        gae_database = "firestore"

        cred = credentials.ApplicationDefault()
        try:
            firebase_admin.initialize_app(cred)
        except Exception as e:
            print("Firebase already initialized.")
elif os.environ.get("APPSETTING_WEBSITE_SITE_NAME"):
    server_env = "azure"
    logging.warning("Platform: Azure")  # only the warning logs (and above) are visible on Azure

    from pymongo import MongoClient
    client = MongoClient(os.getenv("APPSETTING_MONGOURL"))  # this env var should be created automatically when you launch Cosmos DB (with MongoDB API) on Azure
elif os.environ.get("DYNO"):
    server_env = "heroku"
    print("Platform: Heroku")

    from pymongo import MongoClient
    client = MongoClient(os.getenv("MONGODB_URI"))
else:
    server_env = "localhost"
    print("Platform: localhost")
    from tinydb import TinyDB, Query


class Model:
    """
    Every class for database collections should inherit this Model.

    The easiest way to form a class is this:

    class User(Model):
        pass

    Because the class inherits Model, it also inherits its __init__ method, which means you can add any number of fields
    to the object you are creating:

    user = User(name="Matt", surname="Ramuta", age=31)

    But if you want to make some fields mandatory, create the __init__ method in your child class. Example:

    class User(Model):
        def __init__(self, name, **kwargs):
            super().__init__(**kwargs)  # needed because of the parent Model class
            self.name = name

    In this case the 'name' attribute is mandatory, the others are optional (passed via **kwargs).

    """

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def get_collection(cls):
        """
        Get collection (create one if it doesn't exist yet).
        :return: collection reference object
        """
        collection = None

        if server_env == "localhost":
            # datetime serializer for TinyDB. Make sure to add tinydb-serialization in requirements.txt
            from datetime import datetime
            from tinydb_serialization import Serializer, SerializationMiddleware

            class DateTimeSerializer(Serializer):
                OBJ_CLASS = datetime  # The class this serializer handles

                def encode(self, obj):
                    return obj.strftime('%Y-%m-%dT%H:%M:%S')

                def decode(self, s):
                    return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')

            serialization = SerializationMiddleware()
            serialization.register_serializer(DateTimeSerializer(), 'TinyDate')

            tiny_db_name = 'db.json'
            if os.getenv("TESTING"):
                tiny_db_name = 'test_db.json'

            db = TinyDB(tiny_db_name, storage=serialization)
            collection = db.table(cls.__name__)
        elif server_env == "gae":
            if gae_database == "datastore":
                db = datastore.Client()
                return db  # no collections in the Datastore
            else:
                db = firestore.client()
                collection = db.collection(cls.__name__)
        elif server_env == "azure":
            db = client["my-database"]
            # these two env vars should be created automatically when you launch Cosmos DB (with MongoDB API) on Azure
            # if not, add them manually
            db.authenticate(name=os.getenv("APPSETTING_MONGO_USERNAME"),
                            password=os.getenv("APPSETTING_MONGO_PASSWORD"))
            collection = db[cls.__name__]
        elif server_env == "heroku":
            db_name = os.getenv("MONGODB_URI").split(":")[1].replace("//", "")  # get the username out, which is also the db name
            db = client[db_name]
            collection = db[cls.__name__]

        return collection

    def create(self):
        """
        Create a new document and put it in a collection.
        :return: document id

        Example:

        user = User(name=author_name, age=author_age)
        user = user.create()

        """
        collection = self.get_collection()
        obj_id = None

        if server_env == "localhost":
            obj_id = collection.insert(self.__dict__)

        elif server_env == "gae":
            if gae_database == "datastore":
                entity = datastore.Entity(key=collection.key(self.__class__.__name__))
                entity.update(self.__dict__)
                collection.put(entity)

                obj_id = entity.key.id
            else:
                obj_ref = collection.document()
                obj_ref.set(self.__dict__)
                obj_doc = obj_ref.get()

                obj_id = obj_doc.id

        elif server_env == "azure" or server_env == "heroku":
            obj_id = collection.insert_one(self.__dict__).inserted_id

        self.id = obj_id  # save the ID back in the object
        return obj_id

    @classmethod
    def edit(cls, obj_id, **kwargs):
        """
        Edit a document and save the change into a collection.
        :param obj_id: document object id
        :param kwargs: document object updated fields (can take any amount of arguments)
        :return: None

        Example:

        Message.edit(obj_id=message_id, content=message_content)
        """
        collection = cls.get_collection()

        if server_env == "localhost":
            collection.update(kwargs, doc_ids=[int(obj_id)])
        elif server_env == "gae":
            if gae_database == "datastore":
                key = collection.key(cls.__name__, obj_id)
                obj = collection.get(key=key)

                for key, value in kwargs.items():
                    obj[key] = value

                collection.put(obj)
            else:
                collection.document(obj_id).update(kwargs)
        elif server_env == "azure" or server_env == "heroku":
            from bson import ObjectId
            collection.update_one({"_id": ObjectId(obj_id)}, {"$set": kwargs})

    @classmethod
    def delete(cls, obj_id):
        """
        Delete a document.
        :param obj_id: document object id
        :return: None

        Example:

        Message.delete(obj_id=message_id)
        """
        collection = cls.get_collection()

        if server_env == "localhost":
            collection.remove(doc_ids=[int(obj_id)])
        elif server_env == "gae":
            if gae_database == "datastore":
                key = collection.key(cls.__name__, obj_id)
                collection.delete(key=key)
            else:
                collection.document(obj_id).delete()
        elif server_env == "azure" or server_env == "heroku":
            from bson import ObjectId
            collection.delete_one({"_id": ObjectId(obj_id)})

    @classmethod
    def get(cls, obj_id):
        """
        Get a document object from a collection.
        :param obj_id: document object id
        :return: document object

        Example:

        user = User.get(obj_id=user_id)
        """
        collection = cls.get_collection()
        obj = None

        if server_env == "localhost":
            obj_id = int(obj_id)
            obj_dict = collection.get(doc_id=obj_id)
            obj_dict["id"] = obj_id  # add ID to the dict before converting it into an object
            obj = cls(**obj_dict)

        elif server_env == "gae":
            if gae_database == "datastore":
                key = collection.key(cls.__name__, obj_id)
                entity = collection.get(key=key)
                obj = cls(id=entity.key.id, **entity)  # convert datastore entity into an object based on our model
            else:
                obj_doc = collection.document(obj_id).get()
                obj_dict = obj_doc.to_dict()
                obj_dict["id"] = obj_doc.id
                obj = cls(**obj_dict)  # convert from dict into object

        elif server_env == "azure" or server_env == "heroku":
            from bson import ObjectId
            obj_dict = collection.find_one({"_id": ObjectId(obj_id)})
            obj_dict["id"] = obj_id
            del obj_dict["_id"]
            obj = cls(**obj_dict)  # convert from dict into object

        return obj

    @classmethod
    def fetch(cls, limit=0, **kwargs):
        """
        Get many documents from a collection. You can apply a filter or a limit.
        :param limit: limit the amount of documents you receive back
        :param kwargs: filter the documents based on special queries
        :return: a list of document objects

        A query must be formed in this way (the Firestore way): ["name", "==", "Joe"]. The first item is field, the
        second one is operator and the last one is value.

        Examples:

        messages = Message.fetch()  # fetch every document from the Message collection

        users = User.fetch(limit=3, query=["city", "==", "Boston"])  # get 3 users from Boston

        query1 = ["name", "==", "Matej"]
        query2 = ["age", ">", 30]
        query3 = ["deleted", "==", False]
        users = User.fetch(limit=3, query1=query1, query2=query2, query3=query3)  # multiple queries (combined with AND)
        """
        collection = cls.get_collection()
        objects = []

        # check for the correct type and operator
        if kwargs:
            for query_list in kwargs.values():
                if type(query_list) is not list:
                    raise TypeError("The query must be a list!")

                if "!=" in query_list:
                    raise SyntaxError("The '!=' operator is not supported.")  # Firestore does not support this operator

        if server_env == "localhost":
            if kwargs:
                query_string = ""
                for query_list in kwargs.values():
                    if isinstance(query_list[2], str):
                        query_string += "(Query().{0} {1} '{2}') & ".format(query_list[0], query_list[1], query_list[2])
                    else:
                        query_string += "(Query().{0} {1} {2}) & ".format(query_list[0], query_list[1], query_list[2])
                query_string = query_string[:-3]
                dict_objects = collection.search(eval(query_string))
            else:
                dict_objects = collection.all()

            if limit:
                dict_objects = dict_objects[:limit]

            for obj_dict in dict_objects:
                obj_dict["id"] = obj_dict.doc_id  # add ID to the dict before converting it into an object
                obj = cls(**obj_dict)  # convert from dict into object
                objects.append(obj)

        elif server_env == "gae":
            if gae_database == "datastore":
                query = collection.query(kind=cls.__name__)
                if kwargs:
                    for query_list in kwargs.values():
                        if query_list[1] == "==":
                            query_list[1] = "="  # double equals are not allowed in datastore queries

                        query.add_filter(query_list[0], query_list[1], query_list[2])

                if limit:
                    entity_objects = query.fetch(limit=limit)
                else:
                    entity_objects = query.fetch()

                for entity in entity_objects:
                    obj = cls(id=entity.key.id, **entity)  # convert datastore entities into objects based on our model
                    objects.append(obj)
            else:
                query_ref = "collection"
                if kwargs:
                    for query_list in kwargs.values():
                        if isinstance(query_list[2], str):
                            query_ref += ".where('{0}', '{1}', '{2}')".format(query_list[0], query_list[1], query_list[2])
                        else:
                            query_ref += ".where('{0}', '{1}', {2})".format(query_list[0], query_list[1], query_list[2])

                if limit:
                    doc_objects = eval(query_ref).limit(limit).get()
                else:
                    doc_objects = eval(query_ref).get()

                for obj_doc in doc_objects:
                    obj_dict = obj_doc.to_dict()
                    obj_dict["id"] = obj_doc.id  # add ID to the dict before converting it into an object
                    obj = cls(**obj_dict)  # convert from dict into object
                    objects.append(obj)

        elif server_env == "azure" or server_env == "heroku":
            if kwargs:
                query_filter = {}
                and_list = []

                for query_list in kwargs.values():
                    if len(kwargs) == 1:
                        if query_list[1] == ">":
                            query_filter = {query_list[0]: {"$gt": query_list[2]}}
                        elif query_list[1] == "<":
                            query_filter = {query_list[0]: {"$lt": query_list[2]}}
                        elif query_list[1] == ">=":
                            query_filter = {query_list[0]: {"$gte": query_list[2]}}
                        elif query_list[1] == "<=":
                            query_filter = {query_list[0]: {"$lte": query_list[2]}}
                        else:
                            query_filter = {query_list[0]: query_list[2]}
                    else:
                        if query_list[1] == ">":
                            and_list.append({query_list[0]: {"$gt": query_list[2]}})
                        elif query_list[1] == "<":
                            and_list.append({query_list[0]: {"$lt": query_list[2]}})
                        elif query_list[1] == ">=":
                            and_list.append({query_list[0]: {"$gte": query_list[2]}})
                        elif query_list[1] == "<=":
                            and_list.append({query_list[0]: {"$lte": query_list[2]}})
                        else:
                            and_list.append({query_list[0]: query_list[2]})

                if len(kwargs) > 1:
                    query_filter["$and"] = and_list

                if limit:
                    dict_objects = collection.find(query_filter).limit(limit)
                else:
                    dict_objects = collection.find(query_filter)
            else:
                if limit:
                    dict_objects = collection.find().limit(limit)
                else:
                    dict_objects = collection.find()

            for obj_dict in dict_objects:
                obj_dict["id"] = str(obj_dict["_id"])
                del obj_dict["_id"]
                obj = cls(**obj_dict)  # convert from dict into object
                objects.append(obj)

        return objects

    @classmethod
    def fetch_one(cls, **kwargs):
        results = cls.fetch(**kwargs)

        if results:
            return results[0]
        else:
            return None
