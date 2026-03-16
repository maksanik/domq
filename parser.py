import os

root_dir = os.path.dirname(os.path.abspath(__file__))


def print_tree(path, prefix=""):
    items = sorted(os.listdir(path))

    items = [
        item for item in items
        if not item.startswith(".")
        and item != "__pycache__"
        and item != "__init__.py"
    ]

    for i, item in enumerate(items):

        full_path = os.path.join(path, item)
        connector = "└── " if i == len(items) - 1 else "├── "

        print(prefix + connector + item)

        if os.path.isdir(full_path):
            extension = "    " if i == len(items) - 1 else "│   "
            print_tree(full_path, prefix + extension)


if __name__ == "__main__":
    print(os.path.basename(root_dir))
    print_tree(root_dir)