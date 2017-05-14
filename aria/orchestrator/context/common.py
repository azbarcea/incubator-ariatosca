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

"""
A common context for both workflow and operation
"""

import logging
import collections
from contextlib import contextmanager
from functools import partial

import jinja2

from aria import (
    logger as aria_logger,
    modeling
)
from aria.storage import exceptions

from ...utils.uuid import generate_uuid


class BaseContext(object):
    """
    Base context object for workflow and operation
    """

    class PrefixedLogger(object):
        def __init__(self, base_logger, task_id=None):
            self._logger = base_logger
            self._task_id = task_id

        def __getattr__(self, attribute):
            if attribute.upper() in logging._levelNames:
                return partial(self._logger_with_task_id, _level=attribute)
            else:
                return getattr(self._logger, attribute)

        def _logger_with_task_id(self, *args, **kwargs):
            level = kwargs.pop('_level')
            kwargs.setdefault('extra', {})['task_id'] = self._task_id
            return getattr(self._logger, level)(*args, **kwargs)

    def __init__(self,
                 name,
                 service_id,
                 model_storage,
                 resource_storage,
                 execution_id,
                 workdir=None,
                 **kwargs):
        super(BaseContext, self).__init__(**kwargs)
        self._name = name
        self._id = generate_uuid(variant='uuid')
        self._model = model_storage
        self._resource = resource_storage
        self._service_id = service_id
        self._workdir = workdir
        self._execution_id = execution_id
        self.logger = None

    def _register_logger(self, level=None, task_id=None):
        self.logger = self.PrefixedLogger(
            logging.getLogger(aria_logger.TASK_LOGGER_NAME), task_id=task_id)
        self.logger.setLevel(level or logging.DEBUG)
        if not self.logger.handlers:
            self.logger.addHandler(self._get_sqla_handler())

    def _get_sqla_handler(self):
        return aria_logger.create_sqla_log_handler(model=self._model,
                                                   log_cls=modeling.models.Log,
                                                   execution_id=self._execution_id)

    def __repr__(self):
        return (
            '{name}(name={self.name}, '
            'deployment_id={self._service_id}, '
            .format(name=self.__class__.__name__, self=self))

    @contextmanager
    def logging_handlers(self, handlers=None):
        handlers = handlers or []
        try:
            for handler in handlers:
                self.logger.addHandler(handler)
            yield self.logger
        finally:
            for handler in handlers:
                self.logger.removeHandler(handler)

    @property
    def model(self):
        """
        Access to the model storage
        :return:
        """
        return self._model

    @property
    def resource(self):
        """
        Access to the resource storage
        :return:
        """
        return self._resource

    @property
    def service_template(self):
        """
        The blueprint model
        """
        return self.service.service_template

    @property
    def service(self):
        """
        The deployment model
        """
        return self.model.service.get(self._service_id)

    @property
    def name(self):
        """
        The operation name
        :return:
        """
        return self._name

    @property
    def id(self):
        """
        The operation id
        :return:
        """
        return self._id

    def download_resource(self, destination, path=None):
        """
        Download a blueprint resource from the resource storage
        """
        try:
            self.resource.service.download(entry_id=str(self.service.id),
                                           destination=destination,
                                           path=path)
        except exceptions.StorageError:
            self.resource.service_template.download(entry_id=str(self.service_template.id),
                                                    destination=destination,
                                                    path=path)

    def download_resource_and_render(self, destination, path=None, variables=None):
        """
        Download a blueprint resource from the resource storage render its content as a jinja
        template using the provided variables. ctx is available to the template without providing it
        explicitly.
        """
        resource_content = self.get_resource(path=path)
        resource_content = self._render_resource(resource_content=resource_content,
                                                 variables=variables)
        with open(destination, 'wb') as f:
            f.write(resource_content)

    def get_resource(self, path=None):
        """
        Read a deployment resource as string from the resource storage
        """
        try:
            return self.resource.service.read(entry_id=str(self.service.id), path=path)
        except exceptions.StorageError:
            return self.resource.service_template.read(entry_id=str(self.service_template.id),
                                                       path=path)

    def get_resource_and_render(self, path=None, variables=None):
        """
        Read a deployment resource as string from the resource storage and render it as a jinja
        template using the provided variables. ctx is available to the template without providing it
        explicitly.
        """
        resource_content = self.get_resource(path=path)
        return self._render_resource(resource_content=resource_content, variables=variables)

    def _render_resource(self, resource_content, variables):
        variables = variables or {}
        variables.setdefault('ctx', self)
        resource_template = jinja2.Template(resource_content)
        return resource_template.render(variables)

    def _teardown_db_resources(self):
        self.model.log._session.close()
        self.model.log._engine.dispose()

class _Dict(collections.MutableMapping):
    def __init__(self, actor, model, nested=None):
        super(_Dict, self).__init__()
        self._actor = actor
        self._attributes = self._actor.attributes
        self._model = model
        self._attr_cls = self._model.parameter.model_cls
        self._nested = nested or []

    def __delitem__(self, key):
        del self._nested_value[key]

    def __contains__(self, item):
        for key in self.keys():
            if item == key:
                return True
        return False

    def __len__(self):
        return len(self._nested_value)

    def __nonzero__(self):
        return bool(self._nested_value)

    def __getitem__(self, item):
        if self._nested:
            value = self._nested_value[item]
        else:
            value = self._attributes[item].value
        if isinstance(value, dict):
            return _Dict(self._actor, self._model, nested=self._nested + [item])
        elif isinstance(value, self._attr_cls):
            return value.value
        return value

    def __setitem__(self, key, value):
        if self._nested or key in self._attributes:
            attribute = self._update_attr(key, value)
            self._model.parameter.update(attribute)
        else:
            attr = self._attr_cls.wrap(key, value)
            self._attributes[key] = attr
            self._model.parameter.put(attr)

    @property
    def _nested_value(self):
        current = self._attributes
        for k in self._nested:
            current = current[k]
        return current.value if isinstance(current, self._attr_cls) else current

    def _update_attr(self, key, value):
        current = self._attributes

        # If this is nested, lets extract the Parameter itself
        if self._nested:
            attribute = current = current[self._nested[0]]
            for k in self._nested[1:]:
                current = current[k]
            if isinstance(current, self._attr_cls):
                current.value[key] = value
            else:
                current[key] = value
        elif isinstance(current[key], self._attr_cls):
            attribute = current[key]
            attribute.value = value
        else:
            raise BaseException()

        # Since this a user defined parameter, this doesn't track changes. So we override the entire
        # thing.
        if isinstance(attribute.value, dict):
            value = attribute.value.copy()
            attribute.value.clear()
        attribute.value = value
        return attribute

    def _unwrap(self, attr):
        return attr.unwrap() if isinstance(attr, self._attr_cls) else attr

    def keys(self):
        dict_ = (self._nested_value.value
                 if isinstance(self._nested_value, self._attr_cls)
                 else self._nested_value)
        for key in dict_.keys():
            yield key

    def values(self):
        for val in self._nested_value.values():
            if isinstance(val, self._attr_cls):
                yield val.value
            else:
                yield val

    def items(self):
        for key in self._nested_value:
            val = self._nested_value[key]
            if isinstance(val, self._attr_cls):
                yield key, val.value
            else:
                yield key, val

    def __dict__(self):
        return dict(item for item in self.items())

    def __iter__(self):
        for key in self._nested_value.keys():
            yield key

    def __copy__(self):
        return dict((k, v) for k, v in self.items())

    def __deepcopy__(self, *args, **kwargs):
        return self.__copy__()

    def copy(self):
        return self.__copy__()

    def clear(self):
        self._nested_value.clear()

    def update(self, dict_=None, **kwargs):
        if dict_:
            for key, value in dict_.items():
                self[key] = value

        for key, value in kwargs.items():
            self[key] = value


class DecorateAttributes(dict):

    def __init__(self, func):
        super(DecorateAttributes, self).__init__()
        self._func = func
        self._attributes = None
        self._actor = None

    @property
    def attributes(self):
        return self._attributes

    @property
    def actor(self):
        return self._actor

    def __getattr__(self, item):
        try:
            return getattr(self._actor, item)
        except AttributeError:
            return super(DecorateAttributes, self).__getattribute__(item)

    def __call__(self, *args, **kwargs):
        func_self = args[0]
        self._actor = self._func(*args, **kwargs)
        self._attributes = _Dict(self._actor, func_self.model)
        return self
