import os
import ast
import sys
from time import sleep

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), ".."
)))

from common.utils import import_module

def parse_classes_from_file(file_path):
    with open(file_path, 'r') as file:
        node = ast.parse(file.read(), filename=file_path)
    
    classes = {}
    for item in node.body:
        if isinstance(item, ast.ClassDef):
            class_name = item.name
            bases = [base.id for base in item.bases if isinstance(base, ast.Name)]
            abstract = False
            if "ABC" in bases:
                abstract = True
                bases.remove("ABC")
            # Find the name attribute
            name = ""
            for class_item in item.body:
                if isinstance(class_item, ast.Assign):
                    for target in class_item.targets:
                        if isinstance(target, ast.Name) and target.id == "name":
                            # Assuming the value is a simple string
                            value = ast.literal_eval(class_item.value)
                            name = value
            classes[class_name] = {"filepath": file_path, "bases": bases, "abstract": abstract, "name": name, "last-modified": os.path.getmtime(file_path)}
    return classes

def build_class_hierarchy(folder_path):
    class_hierarchy = {}

    # Use os.walk to traverse the directory and its subdirectories
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith('.py'):
                file_path = os.path.join(dirpath, filename)
                classes = parse_classes_from_file(file_path)
                class_hierarchy.update(classes)
    
    return class_hierarchy

def print_hierarchy(class_hierarchy, class_name, level=0):
    if class_name not in class_hierarchy:
        print(f"Class '{class_name}' not found in the hierarchy.")
        return
    
    print('  ' * level + f'- {class_name}')
    for cls, cls_val in class_hierarchy.items():
        if class_name in cls_val["bases"]:
            print_hierarchy(class_hierarchy, cls, level + 1)

def import_class_hierarchy(folder_path):
    class_hierarchy = build_class_hierarchy(folder_path)
    for cls_key, cls_val in class_hierarchy.items():
        _, module = import_module(cls_val["filepath"])
        cls_object = getattr(module, cls_key)
        cls_val["obj"] = cls_object
    return class_hierarchy

def mermaid_graph(class_hierarchy: dict):
    repr = "classDiagram\n"
    for cls_name, cls_val in class_hierarchy.items():
        repr += f"\tclass {cls_name}\n"
        for base_name in cls_val["bases"]:
            repr += f"\t{base_name} <|-- {cls_name}\n"
    return repr
