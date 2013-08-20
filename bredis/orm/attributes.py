import time
from datetime import datetime, date

from .. import is_async
from .exceptions import FieldValidationError


class Attribute(object):
    """Defines an attribute of the model.

    The attribute accepts strings and are stored in Redis as
    they are - strings.
    """
    def __init__(self, name=None, required=False, default=None, validator=None):
        self.name = name
        self.required = required
        self.default = default
        self.validator = validator

    def __get__(self, instance):
        try:
            return getattr(instance, '_' + self.name)
        except AttributeError:
            if instance.is_new() or is_async():
                value = self.default
            else:
                value = instance.db.hget(instance.key(), self.name)
                if value is not None:
                    value = self.typecast_for_read(value)

            self.__set__(instance, value)
            return value

    def __set__(self, instance, value):
        setattr(instance, '_' + self.name, value)

    def typecast_for_read(self, value):
        """Typecasts the value for reading from Redis."""

        if isinstance(value, unicode):
            return value
        # The redis client encodes all unicode data to utf-8 by default.
        return value.decode('utf-8')

    def typecast_for_storage(self, value):
        """Typecasts the value for storing to Redis."""

        try:
            return unicode(value)
        except UnicodeError:
            return value.decode('utf-8')

    def value_type(self):
        return unicode

    def acceptable_types(self):
        return basestring

    def validate(self, instance):
        """Validate the value."""

        errors = []
        value = getattr(instance, self.name)

        if value and not isinstance(value, self.acceptable_types()):
            errors.append((self.name, 'bad type'))

        if self.required:
            if value is None or not unicode(value).strip():
                errors.append((self.name, 'required'))

        if self.validator:
            errs = self.validator(self.name, value)
            if errs:
                errors.extend(errs)

        if errors:
            raise FieldValidationError(errors)


class CharField(Attribute):
    """Model field of str and unicode type."""

    def __init__(self, max_length=2048, **kwargs):
        super(CharField, self).__init__(**kwargs)
        self.max_length = max_length

    def validate(self, instance):
        errors = []

        try:
            super(CharField, self).validate(instance)
        except FieldValidationError as e:
            errors.extend(e.errors)

        value = getattr(instance, self.name)

        if value and len(value) > self.max_length:
            errors.append((self.name, 'exceeds max length'))

        if errors:
            raise FieldValidationError(errors)


class BooleanField(Attribute):
    """Model field of bool type."""

    def typecast_for_read(self, value):
        return bool(int(value))

    def typecast_for_storage(self, value):
        return "1" if value else "0"

    def value_type(self):
        return bool

    def acceptable_types(self):
        return self.value_type()


class IntegerField(Attribute):
    """Model field of int and long type."""

    def typecast_for_read(self, value):
        return int(value)

    def typecast_for_storage(self, value):
        if value is None:
            return "0"
        return unicode(value)

    def value_type(self):
        return int

    def acceptable_types(self):
        return (int, long)


class FloatField(Attribute):
    """Model field of float type."""

    def typecast_for_read(self, value):
        return float(value)

    def typecast_for_storage(self, value):
        if value is None:
            return "0"
        return "%f" % value

    def value_type(self):
        return float

    def acceptable_types(self):
        return self.value_type()


class DateTimeField(Attribute):
    """Model field of datetime object."""

    def __init__(self, auto_now=False, auto_now_add=False, **kwargs):
        super(DateTimeField, self).__init__(**kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add

    def typecast_for_read(self, value):
        try:
            return datetime.fromtimestamp(int(value))
        except TypeError, ValueError:
            return None

    def typecast_for_storage(self, value):
        if not isinstance(value, datetime):
            raise TypeError("%s should be datetime object, and not a %s" %
                            (self.name, type(value)))
        if value is None:
            return None
        return unicode(int(time.mktime(value.timetuple())))

    def value_type(self):
        return datetime

    def acceptable_types(self):
        return self.value_type()


class DateField(Attribute):
    """Model field of date object."""

    def __init__(self, auto_now=False, auto_now_add=False, **kwargs):
        super(DateField, self).__init__(**kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add

    def typecast_for_read(self, value):
        try:
            return date.fromtimestamp(int(value))
        except TypeError, ValueError:
            return None

    def typecast_for_storage(self, value):
        if not isinstance(value, date):
            raise TypeError("%s should be date object, and not a %s" %
                    (self.name, type(value)))
        if value is None:
            return None
        return unicode(int(time.mktime(value.timetuple())))

    def value_type(self):
        return date

    def acceptable_types(self):
        return self.value_type()


class ReferenceField(object):
    """Model field of reference."""

    def __init__(self, target_type,
                 name=None, attname=None, required=False, related_name=None,
                 default=None, validator=None):
        # store name_id (object id) and _name (referenced object)
        self.name = name
        self.required = required
        self.default = default
        self.validator = validator

        self._target_type = target_type
        self._attname = attname
        self._related_name = related_name

    def __get__(self, instance):
        try:
            if not hasattr(instance, '_' + self.name):
                id = getattr(instance, self.attname)
                if is_async():  # not fetch object on async mode
                    setattr(instance, '_' + self.name, id)
                else:
                    setattr(instance, '_' + self.name,
                            self.value_type().objects.get_by_id(id))
            return getattr(instance, '_' + self.name)
        except AttributeError:
            setattr(instance, '_' + self.name, self.default)
            return self.default

    def __set__(self, instance, value):
        # store object id or None
        if not str(value).isdigit() and value is not None:
            if isinstance(value, self.value_type()):
                value = value.id
            else:
                raise TypeError

        if hasattr(instance, '_' + self.name):
            delattr(instance, '_' + self.name)
        setattr(instance, self.attname, str(value) if value else None)

    def value_type(self):
        return self._target_type

    @property
    def attname(self):
        if self._attname is None:
            self._attname = self.name + '_id'
        return self._attname

    @property
    def related_name(self):
        return self._related_name

    def validate(self, instance):
        errors = []
        value = getattr(instance, self.attname, None)

        if value and not str(value).isdigit():
            errors.append((self.name, 'bad type for reference'))

        if self.required:
            if not value:
                errors.append((self.name, 'required'))

        if self.validator:
            errs = self.validator(self.name, value)
            if errs:
                errors.extend(errs)

        if errors:
            raise FieldValidationError(errors)


class Counter(IntegerField):

    def __init__(self, **kwargs):
        super(Counter, self).__init__(**kwargs)
        if not kwargs.has_key('default') or self.default is None:
            self.default = 0

    def __get__(self, instance):
        if not (instance.is_new() or is_async()):
            value = instance.db.hget(instance.key(), self.name)
            if value is None:
                return 0
            return int(value)
        else:
            return 0

    def __set__(self, instance, value):
        raise AttributeError("can't set a counter.")

