from os import linesep
from typing import List, Dict, Set
from functools import partial

from google.protobuf.reflection import GeneratedProtocolMessageType
from google.protobuf.descriptor import Descriptor, FieldDescriptor, EnumDescriptor

from networkx import DiGraph


tab = " " * 4
one_line, two_lines = linesep * 2, linesep * 3
type_mapping = {
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT64: int,
    FieldDescriptor.TYPE_UINT64: int,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_FIXED64: float,
    FieldDescriptor.TYPE_FIXED32: float,
    FieldDescriptor.TYPE_BOOL: bool,
    FieldDescriptor.TYPE_STRING: str,
    FieldDescriptor.TYPE_BYTES: str,
    FieldDescriptor.TYPE_UINT32: int,
    FieldDescriptor.TYPE_SFIXED32: float,
    FieldDescriptor.TYPE_SFIXED64: float,
    FieldDescriptor.TYPE_SINT32: int,
    FieldDescriptor.TYPE_SINT64: int,
}


def m(field: FieldDescriptor) -> str:
    return type_mapping[field.type].__name__


def convert_field(level: int, known_msg_types: list, field: FieldDescriptor,) -> str:
    level += 1
    field_type = field.type
    extra = None
    encoder = field._encoder.__qualname__.split('.')[0]
    is_list = 'Repeated' in field._default_constructor.__qualname__
    is_part_of_oneof = field.containing_oneof is not None

    if field_type == FieldDescriptor.TYPE_ENUM:
        enum_type: EnumDescriptor = field.enum_type
        type_statement = enum_type.name
        class_statement = f"{tab * level}class {enum_type.name}(Enum):"
        field_statements = map(
            lambda value: f'{tab * (level + 1)}{value.name} = "{value.name}"',
            enum_type.values,
        )
        extra = linesep.join([class_statement, *field_statements])
        factory = "int"
    elif field_type == FieldDescriptor.TYPE_MESSAGE:
        type_statement: str = field.message_type.name
        if type_statement in known_msg_types:
            factory = type_statement
        elif type_statement == "Struct":
            type_statement = "Dict[str, Any]"
            factory = "dict"
        elif type_statement == "Timestamp":
            type_statement = "datetime"
            factory = "datetime"
        elif encoder == "MapEncoder":
            key, value = field.message_type.fields  # type: FieldDescriptor

            if value.type == FieldDescriptor.TYPE_MESSAGE:
                if value.message_type.name not in known_msg_types:
                    raise Exception(f"Unknown type '{value.message_type.name}' for Map field '{field.containing_type.name}.{field.name}' value")
                value_type_name = value.message_type.name
            elif value.type in type_mapping:
                value_type_name = type_mapping[value.type].__name__
            else:
                raise Exception(f"Unknown type ID '{value.type}' for Map field '{field.name}' value")

            if key.type not in type_mapping:
                raise Exception(f"Unknown type {key.type} for Map field '{field.name}' key")
            key_type_name = type_mapping[key.type].__name__

            type_statement = f"Dict[{key_type_name}, {value_type_name}]"
            factory = "dict"
        else:
            raise Exception(f"Unknown type {type_statement}")
    else:
        type_statement = m(field)
        factory = type_statement

    if is_list:
        # repeated is NOT allowed in dicts, not sure why it has the repeated label
        type_statement = f"List[{type_statement}]"
        factory = "list"

    if is_part_of_oneof:
        type_statement = f"Optional[{type_statement}]"

    # default_statement = f" = Field(default_factory={factory})"
    # if field_label == FieldDescriptor.LABEL_REQUIRED:
    #    default_statement = ""

    default_statement = ""

    field_statement = f"{tab * level}{field.name}: {type_statement}{default_statement}"
    if not extra:
        return field_statement
    return linesep + extra + one_line + field_statement


KNOWN_MESSAGES = {"Timestamp"}


def resolve_dependencies(module) -> str:
    dependencies = DiGraph()
    for i in dir(module):
        obj = getattr(module, i)
        if not isinstance(obj, GeneratedProtocolMessageType):
            continue

        dependencies.add_node(obj.DESCRIPTOR.name)
        for field in obj.DESCRIPTOR.fields:
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                type_name = field.message_type.name
                encoder = field._encoder.__qualname__.split('.')[0]
                # Heuristc way of finding out if this message field refernces
                # another message. Other field types like maps will have other encoders
                # This way de don't rely on the "endswith("Entry")", that may not work if
                # the message name actually ends with Entry
                if encoder == "MessageEncoder":
                    if type_name not in KNOWN_MESSAGES:
                        dependencies.add_edge(obj.DESCRIPTOR.name, type_name)

                # We may have a type used in the value field of a map
                if encoder == "MapEncoder":
                    key, value = field.message_type.fields  # type: FieldDescriptor
                    if value.type == FieldDescriptor.TYPE_MESSAGE and value.message_type.name not in KNOWN_MESSAGES:
                        dependencies.add_edge(obj.DESCRIPTOR.name, value.message_type.name)

    return dependencies


def walk_dependencies(dependencies: DiGraph):
    dependants = [node for node in dependencies.nodes() if dependencies.in_degree(node) == 0]
    visited = set()

    def recurse(items: list, level, parent):
        # print("\n" + "  " * level + f"- {parent}")
        for item in items:
            if item not in visited:
                visited.add(item)

                successors = list(dependencies.successors(item))
                if successors:
                    yield from recurse(successors, level + 1, item)

                yield item

    yield from recurse(dependants, 0, "")

def msg2pydantic(level: int, msg: Descriptor, known_msg_types: list) -> str:
    class_statement = f"{tab * level}class {msg.name}(BaseModel):"
    field_statements = map(partial(convert_field, level, known_msg_types), msg.fields)
    return linesep.join([class_statement, *field_statements])


def get_config(level: int):
    level += 1
    class_statement = f"{tab * level}class Config:"
    attribute_statement = f"{tab * (level + 1)}arbitrary_types_allowed = True"
    return linesep + class_statement + linesep + attribute_statement


def pb2_to_pydantic(module) -> str:
    pydantic_models: List[str] = []

    dependencies = resolve_dependencies(module)
    processed = []
    for msg_name in walk_dependencies(dependencies):
        #print(f"Processing '{msg_name}', done: {str(processed)}")
        obj = getattr(module, msg_name)
        model_string = msg2pydantic(0, obj.DESCRIPTOR, processed)
        processed.append(msg_name)
        pydantic_models.append(model_string)

    header = """from typing import List, Dict, Optional
from enum import Enum
from datetime import datetime

from pydantic import BaseModel


"""
    return header + two_lines.join(pydantic_models)
