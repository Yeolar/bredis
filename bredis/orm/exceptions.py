class Error(Exception):
    pass

class ValidationError(Error):
    pass

class MissingID(Error):
    pass

class BadKeyError(Error):
    pass

class AttributeNotIndexed(Error):
    pass

class FieldValidationError(Error):

    def __init__(self, errors, *args, **kwargs):
        super(FieldValidationError, self).__init__(*args, **kwargs)
        self._errors = errors

    @property
    def errors(self):
        return self._errors
