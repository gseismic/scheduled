import importlib


def join_module_path(part1, part2, part3=None):
    path = '.'.join([part1.rstrip('.'), part2.lstrip('.')])
    if part3:
        path = '.'.join([path.rstrip('.'), part3.lstrip('.')])
    return path


# https://stackoverflow.com/questions/4821104/dynamic-instantiation-from-string-name-of-a-class-in-dynamically-imported-module/4821120#4821120
def split_moduleclass_path(dot_path):
    # package.module.DemoClass
    item = dot_path.split('.')
    module_name, class_name = '.'.join(item[:-1]), item[-1]
    return module_name, class_name


def get_module_and_attr(module_name, class_name):
    '''
    package.module, DemoClass
    '''
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return module, cls


def classname_to_configname(class_name):
    # 'DemoClass' to 'demo_class'
    name = []
    for i, s in enumerate(class_name):
        if i == 0:
            name.append(s.lower())
        elif s.isupper():
            name.append('_' + s.lower())
        elif s.isidentifier():
            name.append(s.lower())
        else:
            raise Exception('Invalid class_name')
    return ''.join(name)


if __name__ == '__main__':
    def test_module_class():
        from settings import KEY_YIELDER
        path = '.key_yielder.KeyYielder'
        module_name, class_name = split_moduleclass_path(path)
        print(module_name, class_name)
        module, cls = get_module_and_attr(module_name, class_name)
        print(module, cls)

    test_module_class()

    v = classname_to_configname('ClassUpper')
    assert v == 'class_upper'

    v = classname_to_configname('Class_upper')
    assert v == 'class_upper'

    v = classname_to_configname('_class_upper')
    assert v == '_class_upper'
