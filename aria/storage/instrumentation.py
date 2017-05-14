# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import json
import os

import sqlalchemy.event

from ..modeling import models as _models
from ..storage.exceptions import StorageError


_VERSION_ID_COL = 'version'
_STUB = object()
_Collection = type('_Collection', (object, ), {})

collection = _Collection()
_INSTRUMENTED = {
    'modified': {
        _models.Node.state: str,
        _models.Task.status: str,
        _models.Node.attributes: collection,
        # TODO: add support for pickled type
        # _models.Parameter._value: some_type
    },
    'new': (_models.Log, ),

}

_NEW_INSTANCE = 'NEW_INSTANCE'


def track_changes(model=None, instrumented=None):
    """Track changes in the specified model columns

    This call will register event listeners using sqlalchemy's event mechanism. The listeners
    instrument all returned objects such that the attributes specified in ``instrumented``, will
    be replaced with a value that is stored in the returned instrumentation context
    ``tracked_changes`` property.

    Why should this be implemented when sqlalchemy already does a fantastic job at tracking changes
    you ask? Well, when sqlalchemy is used with sqlite, due to how sqlite works, only one process
    can hold a write lock to the database. This does not work well when ARIA runs tasks in
    subprocesses (by the process executor) and these tasks wish to change some state as well. These
    tasks certainly deserve a chance to do so!

    To enable this, the subprocess calls ``track_changes()`` before any state changes are made.
    At the end of the subprocess execution, it should return the ``tracked_changes`` attribute of
    the instrumentation context returned from this call, to the parent process. The parent process
    will then call ``apply_tracked_changes()`` that resides in this module as well.
    At that point, the changes will actually be written back to the database.

    :param model: the model storage. it should hold a mapi for each model. the session of each mapi
    is needed to setup events
    :param instrumented: A dict from model columns to their python native type
    :return: The instrumentation context
    """
    return _Instrumentation(model, instrumented or _INSTRUMENTED)


class _Instrumentation(object):

    def __init__(self, model, instrumented):
        self.tracked_changes = {}
        self.new_instances_as_dict = {}
        self.listeners = []
        self._instances_to_expunge = []
        self._model = model
        self._track_changes(instrumented)

    @property
    def _new_instance_id(self):
        return '{prefix}_{index}'.format(prefix=_NEW_INSTANCE,
                                         index=len(self._instances_to_expunge))

    def expunge_session(self):
        for new_instance in self._instances_to_expunge:
            self._get_session_from_model(new_instance.__tablename__).expunge(new_instance)

    def _get_session_from_model(self, tablename):
        mapi = getattr(self._model, tablename, None)
        if mapi:
            return mapi._session
        raise StorageError("Could not retrieve session for {0}".format(tablename))

    def _track_changes(self, instrumented):
        instrumented_attribute_classes = {}
        # Track any newly created instances.
        for instrumented_class in instrumented.get('new', []):
            self._register_new_instance_listener(instrumented_class)

        # Track any newly-set attributes.
        for instrumented_attribute, attribute_type in instrumented.get('modified', {}).items():
            self._register_attribute_listener(instrumented_attribute=instrumented_attribute,
                                              attribute_type=attribute_type)
            # TODO: Revisit this, why not?
            if not isinstance(attribute_type, _Collection):
                instrumented_class = instrumented_attribute.parent.entity
                instrumented_class_attributes = instrumented_attribute_classes.setdefault(
                    instrumented_class, {})
                instrumented_class_attributes[instrumented_attribute.key] = attribute_type

        # Track any global instance update such as 'refresh' or 'load'
        for instrumented_class, instrumented_attributes in instrumented_attribute_classes.items():
            self._register_instance_listeners(instrumented_class=instrumented_class,
                                              instrumented_attributes=instrumented_attributes)

    def _register_new_instance_listener(self, instrumented_class):
        if self._model is None:
            raise StorageError("In order to keep track of new instances, a ctx is needed")

        def listener(_, instance):
            if not isinstance(instance, instrumented_class):
                return
            self._instances_to_expunge.append(instance)
            tracked_instances = self.new_instances_as_dict.setdefault(instance.__modelname__, {})
            tracked_attributes = tracked_instances.setdefault(self._new_instance_id, {})
            instance_as_dict = instance.to_dict()
            instance_as_dict.update((k, getattr(instance, k))
                                    for k in getattr(instance, '__private_fields__', []))
            tracked_attributes.update(instance_as_dict)
        session = self._get_session_from_model(instrumented_class.__tablename__)
        listener_args = (session, 'after_attach', listener)
        sqlalchemy.event.listen(*listener_args)
        self.listeners.append(listener_args)

    def _register_attribute_listener(self, instrumented_attribute, attribute_type):
        # Track and newly created instances that are a part of a collection.
        if isinstance(attribute_type, _Collection):
            return self._register_append_to_attribute_listener(instrumented_attribute)
        else:
            return self._register_set_attribute_listener(instrumented_attribute, attribute_type)

    def _register_append_to_attribute_listener(self, collection_attr):
        def listener(target, value, initiator):
            tracked_instances = self.tracked_changes.setdefault(target.__modelname__, {})
            tracked_attributes = tracked_instances.setdefault(target.id, {})
            collection = tracked_attributes.setdefault(initiator.key, [])
            instance_as_dict = value.to_dict()
            instance_as_dict.update((k, getattr(value, k))
                                    for k in getattr(value, '__private_fields__', []))
            instance_as_dict['_MODEL_CLS'] = value.__modelname__
            collection.append(instance_as_dict)

        listener_args = (collection_attr, 'append', listener)
        sqlalchemy.event.listen(*listener_args)
        self.listeners.append(listener_args)

    def _register_set_attribute_listener(self, instrumented_attribute, attribute_type):
        def listener(target, value, *_):
            mapi_name = target.__modelname__
            tracked_instances = self.tracked_changes.setdefault(mapi_name, {})
            tracked_attributes = tracked_instances.setdefault(target.id, {})
            if value is None:
                current = None
            else:
                current = copy.deepcopy(attribute_type(value))
            tracked_attributes[instrumented_attribute.key] = _Value(_STUB, current)
            return current
        listener_args = (instrumented_attribute, 'set', listener)
        sqlalchemy.event.listen(*listener_args, retval=True)
        self.listeners.append(listener_args)

    def _register_instance_listeners(self, instrumented_class, instrumented_attributes):
        def listener(target, *_):
            mapi_name = instrumented_class.__modelname__
            tracked_instances = self.tracked_changes.setdefault(mapi_name, {})
            tracked_attributes = tracked_instances.setdefault(target.id, {})
            if hasattr(target, _VERSION_ID_COL):
                # We want to keep track of the initial version id so it can be compared
                # with the committed version id when the tracked changes are applied
                tracked_attributes.setdefault(_VERSION_ID_COL,
                                              _Value(_STUB, getattr(target, _VERSION_ID_COL)))
            for attribute_name, attribute_type in instrumented_attributes.items():
                if attribute_name not in tracked_attributes:
                    initial = getattr(target, attribute_name)
                    if initial is None:
                        current = None
                    else:
                        current = copy.deepcopy(attribute_type(initial))
                    tracked_attributes[attribute_name] = _Value(initial, current)
                target.__dict__[attribute_name] = tracked_attributes[attribute_name].current
        for listener_args in ((instrumented_class, 'load', listener),
                              (instrumented_class, 'refresh', listener),
                              (instrumented_class, 'refresh_flush', listener)):
            sqlalchemy.event.listen(*listener_args)
            self.listeners.append(listener_args)

    def clear(self, target=None):
        if target:
            mapi_name = target.__modelname__
            tracked_instances = self.tracked_changes.setdefault(mapi_name, {})
            tracked_instances.pop(target.id, None)
        else:
            self.tracked_changes.clear()

        self.new_instances_as_dict.clear()
        self._instances_to_expunge = []

    def restore(self):
        """Remove all listeners registered by this instrumentation"""
        for listener_args in self.listeners:
            if sqlalchemy.event.contains(*listener_args):
                sqlalchemy.event.remove(*listener_args)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.restore()


class _Value(object):
    # You may wonder why is this a full blown class and not a named tuple. The reason is that
    # jsonpickle that is used to serialize the tracked_changes, does not handle named tuples very
    # well. At the very least, I could not get it to behave.

    def __init__(self, initial, current):
        self.initial = initial
        self.current = current

    def __eq__(self, other):
        if not isinstance(other, _Value):
            return False
        return self.initial == other.initial and self.current == other.current

    def __hash__(self):
        return hash((self.initial, self.current))

    @property
    def dict(self):
        return {'initial': self.initial, 'current': self.current}.copy()


def apply_tracked_changes(tracked_changes, new_instances, model):
    """Write tracked changes back to the database using provided model storage

    :param tracked_changes: The ``tracked_changes`` attribute of the instrumentation context
                            returned by calling ``track_changes()``
    :param model: The model storage used to actually apply the changes
    """
    successfully_updated_changes = dict()
    try:
        # handle instance updates
        for mapi_name, tracked_instances in tracked_changes.items():
            successfully_updated_changes[mapi_name] = dict()
            mapi = getattr(model, mapi_name)

            # Handle new instances
            for mapi_name, new_instance in new_instances.items():
                successfully_updated_changes[mapi_name] = dict()
                mapi = getattr(model, mapi_name)
                for tmp_id, new_instance_kwargs in new_instance.items():
                    instance = mapi.model_cls(**new_instance_kwargs)
                    mapi.put(instance)
                    successfully_updated_changes[mapi_name][instance.id] = new_instance_kwargs
                    new_instance[tmp_id] = instance

            for instance_id, tracked_attributes in tracked_instances.items():
                successfully_updated_changes[mapi_name][instance_id] = dict()
                instance = None
                for attribute_name, value in tracked_attributes.items():
                    instance = instance or mapi.get(instance_id)
                    if isinstance(value, list):
                        # The changes are new item to a collection
                        for item in value:
                            model_name = item.pop('_MODEL_CLS')
                            attr_model = getattr(model, model_name).model_cls
                            new_attr = attr_model(**item)
                            getattr(instance, attribute_name)[new_attr] = new_attr
                    elif value.initial != value.current:
                        # scalar attribute
                        setattr(instance, attribute_name, value.current)
                if instance:
                    _validate_version_id(instance, mapi)
                    mapi.update(instance)
                    # TODO: reinstate this
                    # successfully_updated_changes[mapi_name][instance_id] = [
                    #     v.dict for v in tracked_attributes.values()]

    except BaseException:
        for key, value in successfully_updated_changes.items():
            if not value:
                del successfully_updated_changes[key]
        # TODO: if the successful has _STUB, the logging fails because it can't serialize the object
        model.logger.error(
            'Registering all the changes to the storage has failed. {0}'
            'The successful updates were: {0} '
            '{1}'.format(os.linesep, json.dumps(successfully_updated_changes, indent=4)))

        raise


def _validate_version_id(instance, mapi):
    version_id = sqlalchemy.inspect(instance).committed_state.get(_VERSION_ID_COL)
    # There are two version conflict code paths:
    # 1. The instance committed state loaded already holds a newer version,
    #    in this case, we manually raise the error
    # 2. The UPDATE statement is executed with version validation and sqlalchemy
    #    will raise a StateDataError if there is a version mismatch.
    if version_id and getattr(instance, _VERSION_ID_COL) != version_id:
        object_version_id = getattr(instance, _VERSION_ID_COL)
        mapi._session.rollback()
        raise StorageError(
            'Version conflict: committed and object {0} differ '
            '[committed {0}={1}, object {0}={2}]'
            .format(_VERSION_ID_COL,
                    version_id,
                    object_version_id))
