import time
from datetime import datetime

from tornado import gen
from tornado.util import ObjectDict

from .. import get_client
from ..util import deserialize
from .attributes import *
from .managers import *
from .key import Key
from .exceptions import FieldValidationError, MissingID, BadKeyError


def _initialize_attributes(model_class, name, bases, attrs):
    """Initialize the attributes of the model."""

    model_class._attributes = {}

    # In case of inheritance, we also add the parent's
    # attributes in the list of our attributes
    for parent in bases:
        if not isinstance(parent, ModelBase):
            continue
        for k, v in parent._attributes.iteritems():
            model_class._attributes[k] = v

    for k, v in attrs.iteritems():
        if isinstance(v, Attribute):
            model_class._attributes[k] = v
            v.name = v.name or k


def _initialize_referenced(model_class, attribute):
    """Adds a property to the target of a reference field that
    returns the list of associated objects.
    """
    # this should be a descriptor
    def _related_objects(self):
        return (model_class.objects.filter(**{attribute.attname: self.id}))

    klass = attribute._target_type
    if isinstance(klass, basestring):
        return (klass, model_class, attribute)
    else:
        related_name = (attribute.related_name or
                model_class.__name__.lower() + '_set')
        setattr(klass, related_name, property(_related_objects))


def _initialize_references(model_class, name, bases, attrs):
    """Stores the list of reference field descriptors of a model."""

    model_class._references = {}
    h = {}
    deferred = []

    for parent in bases:
        if not isinstance(parent, ModelBase):
            continue
        for k, v in parent._references.iteritems():
            model_class._references[k] = v
            # We skip updating the attributes since this is done
            # already at the parent construction and then copied back
            # in the subclass
            refd = _initialize_referenced(model_class, v)
            if refd:
                deferred.append(refd)

    for k, v in attrs.iteritems():
        if isinstance(v, ReferenceField):
            model_class._references[k] = v
            v.name = v.name or k
            att = Attribute(name=v.attname)
            h[v.attname] = att
            setattr(model_class, v.attname, att)
            refd = _initialize_referenced(model_class, v)
            if refd:
                deferred.append(refd)

    attrs.update(h)
    return deferred


def _initialize_counters(model_class, name, bases, attrs):
    """Stores the list of counter fields."""

    model_class._counters = []

    for parent in bases:
        if not isinstance(parent, ModelBase):
            continue
        for c in parent._counters:
            model_class._counters.append(c)

    for k, v in attrs.iteritems():
        if isinstance(v, Counter):
            # When subclassing, we want to override the attributes
            if k in model_class._counters:
                model_class._counters.remove(k)
            model_class._counters.append(k)


def _initialize_key(model_class, name):
    """Initializes the key of the model."""

    model_class._key = Key(model_class._meta['key'] or name)


def _initialize_manager(model_class):
    """Initializes the objects manager attribute of the model."""

    model_class.objects = ManagerDescriptor(Manager(model_class))


class ModelOptions(object):
    """Handles options defined in Meta class of the model.

    Example:

        class Person(models.Model):
            name = models.Attribute()

            class Meta:
                db = redis.Redis(host='localhost', port=29909)
    """
    def __init__(self, meta):
        self.meta = meta

    def __getitem__(self, field_name):
        if self.meta is None:
            return None
        try:
            return self.meta.__dict__[field_name]
        except KeyError:
            return None


_deferred_refs = []


class ModelBase(type):
    """Metaclass of the Model."""

    def __init__(cls, name, bases, attrs):
        super(ModelBase, cls).__init__(name, bases, attrs)
        global _deferred_refs
        cls._meta = ModelOptions(attrs.pop('Meta', None))
        deferred = _initialize_references(cls, name, bases, attrs)
        _deferred_refs.extend(deferred)
        _initialize_attributes(cls, name, bases, attrs)
        _initialize_counters(cls, name, bases, attrs)
        _initialize_key(cls, name)
        _initialize_manager(cls)
        # if targeted by a reference field using a string,
        # override for next try
        for target, model_class, att in _deferred_refs:
            if name == target:
                att._target_type = cls
                _initialize_referenced(model_class, att)


class Model(object):

    __metaclass__ = ModelBase

    def __init__(self, **kwargs):
        self.update_attributes(**kwargs)

    def is_valid(self):
        """Returns True if all the fields are valid.

        It first validates the fields (required, unique, etc.)
        and then calls the validate method.
        """
        self._errors = []
        for field in self.fields:
            try:
                field.validate(self)
            except FieldValidationError as e:
                self._errors.extend(e.errors)
        self.validate()
        return not bool(self._errors)

    def validate(self):
        """Overriden in the model class.

        Do custom validation here. Add tuples to self._errors.

        Example:

            class Person(Model):
                name = Attribute(required=True)

                def validate(self):
                    if name == 'Nemo':
                        self._errors.append(('name', 'cannot be Nemo'))
        """
        pass

    def update_attributes(self, **kwargs):
        """Updates the attributes of the model."""

        attrs = self.attributes.values() + self.references.values()
        for att in attrs:
            if att.name in kwargs:
                att.__set__(self, kwargs[att.name])

    def save(self):
        """Saves the instance to the datastore."""

        if not self.is_valid():
            return self._errors
        _new = self.is_new()
        if _new:
            self._initialize_id()
        with Mutex(self):
            self._write(_new)
        return True

    def save_async(self, callback=None):
        """Async save."""

        if not self.is_valid():
            callback(self._errors)
            return
        _new = self.is_new()
        if _new:
            self._initialize_id_async()
        d = self._get_data_for_storage(_new)
        self.db.hmset(self.key(), d, callback)

    def key(self, att=None):
        """Returns the Redis key where the values are stored."""

        if att is not None:
            return self._key[self.id][att]
        else:
            return self._key[self.id]

    def delete(self):
        """Deletes the object from the datastore."""

        pipeline = self.db.pipeline()
        pipeline.delete(self.key())
        pipeline.execute()

    def is_new(self):
        """Returns True if the instance is new.

        Newness is based on the presence of the _id attribute.
        """
        return not hasattr(self, '_id')

    def incr(self, att, val=1):
        """Increments a counter."""
        if att not in self.counters:
            raise ValueError("%s is not a counter.")
        self.db.hincrby(self.key(), att, val)

    def decr(self, att, val=1):
        """Decrements a counter."""
        self.incr(att, -1 * val)

    @property
    def attributes_dict(self):
        """Returns the mapping of the model attributes and their values."""

        h = {}
        for k in self.attributes.keys():
            h[k] = getattr(self, k)
        for k in self.references.keys():
            h[k] = getattr(self, k)
        return h

    @property
    def id(self):
        """Returns the id of the instance.

        Raises MissingID if the instance is new.
        """
        if not hasattr(self, '_id'):
            raise MissingID
        return self._id

    @id.setter
    def id(self, val):
        """Returns the id of the instance as a string."""
        self._id = str(val)

    @property
    def attributes(cls):
        """Return the attributes of the model.

        Returns a dict with models attribute name as keys
        and attribute descriptors as values.
        """
        return dict(cls._attributes)

    @property
    def references(cls):
        """Returns the mapping of reference fields of the model."""
        return cls._references

    @property
    def db(cls):
        """Returns the Redis client used by the model."""
        return get_client()

    @property
    def errors(self):
        """Returns the list of errors after validation."""
        if not hasattr(self, '_errors'):
            self.is_valid()
        return self._errors

    @property
    def fields(self):
        """Returns the list of field names of the model."""
        return (self.attributes.values() + self.references.values())

    @property
    def counters(cls):
        """Returns the mapping of the counters."""
        return cls._counters

    @classmethod
    def exists(cls, id):
        """Checks if the model with id exists."""
        return bool(get_client().exists(cls._key[str(id)]))

    def _initialize_id(self):
        """Initializes the id of the instance."""
        self.id = str(self.db.incr(self._key['id']))

    @gen.engine
    def _initialize_id_async(self):
        """Async initialize."""
        self.id = str(yield gen.Task(self.db.incr, self._key['id']))

    def _write(self, _new=False):
        """Writes the values of the attributes to the datastore.

        This method also creates the indices and saves the lists
        associated to the object.
        """
        pipeline = self.db.pipeline()
        h = {}
        # attributes
        for k, v in self.attributes.iteritems():
            if isinstance(v, DateTimeField):
                if v.auto_now:
                    setattr(self, k, datetime.now())
                if v.auto_now_add and _new:
                    setattr(self, k, datetime.now())
            elif isinstance(v, DateField):
                if v.auto_now:
                    setattr(self, k, datetime.now())
                if v.auto_now_add and _new:
                    setattr(self, k, datetime.now())
            for_storage = getattr(self, k)
            if for_storage is not None:
                h[k] = v.typecast_for_storage(for_storage)

        pipeline.delete(self.key())
        if h:
            pipeline.hmset(self.key(), h)

        pipeline.execute()

    def __hash__(self):
        return hash(self.key())

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.key() == other.key()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        if not self.is_new():
            return "<%s %s>" % (self.key(), self.attributes_dict)
        return "<%s %s>" % (self.__class__.__name__, self.attributes_dict)

    def _get_data_for_storage(self, _new=False):
        h = {}
        for k, v in self.attributes.iteritems():
            if isinstance(v, DateTimeField):
                if v.auto_now:
                    setattr(self, k, datetime.now())
                if v.auto_now_add and _new:
                    setattr(self, k, datetime.now())
            elif isinstance(v, DateField):
                if v.auto_now:
                    setattr(self, k, datetime.now())
                if v.auto_now_add and _new:
                    setattr(self, k, datetime.now())
            if isinstance(v, ReferenceField):
                for_storage = getattr(self, k + '_id', None)
            else:
                for_storage = getattr(self, k)
            if for_storage is not None:
                h[k] = v.typecast_for_storage(for_storage)
        return h

    def typecast_for_read(self, d):
        if d is None:
            return None

        attrs = self.attributes.values()
        for att in attrs:
            if att.name in d:
                if isinstance(d[att.name], (type(None), tuple, list, dict)):
                    att.__set__(self, d[att.name])
                else:
                    att.__set__(self, att.typecast_for_read(d[att.name]))
        if 'id' in d:
            self.id = d['id']
        return self

    @property
    def object_dict(self):
        d = {}
        attrs = self.attributes.values()
        for att in attrs:
            value = getattr(self, att.name)
            if isinstance(value, (str, unicode)):
                d[att.name] = deserialize(value, ignore_error=True)
            else:
                d[att.name] = value
        d['id'] = self.id
        return ObjectDict(d)


def get_model_from_key(key):
    """Gets the model from a given key."""

    _known_models = {}
    model_name = key.split(':', 2)[0]
    # populate
    for klass in Model.__subclasses__():
        _known_models[klass.__name__] = klass
    return _known_models.get(model_name, None)


def from_key(key):
    """Returns the model instance based on the key.

    Raises BadKeyError if the key is not recognized by
    redisco or no defined model can be found.
    Returns None if the key could not be found.
    """
    model = get_model_from_key(key)
    if model is None:
        raise BadKeyError
    try:
        _, id = key.split(':', 2)
        id = int(id)
    except ValueError, TypeError:
        raise BadKeyError
    return model.objects.get_by_id(id)


class Mutex(object):
    """Implements locking so that other instances may not modify it.

    Code ported from Ohm.
    """
    def __init__(self, instance):
        self.instance = instance

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.unlock()

    def lock(self):
        o = self.instance
        while not o.db.setnx(o.key('_lock'), self.lock_timeout):
            lock = o.db.get(o.key('_lock'))
            if not lock:
                continue
            if not self.lock_has_expired(lock):
                time.sleep(0.5)
                continue
            lock = o.db.getset(o.key('_lock'), self.lock_timeout)
            if not lock:
                break
            if self.lock_has_expired(lock):
                break

    def lock_has_expired(self, lock):
        return float(lock) < time.time()

    def unlock(self):
        self.instance.db.delete(self.instance.key('_lock'))

    @property
    def lock_timeout(self):
        return "%f" % (time.time() + 1.0)

