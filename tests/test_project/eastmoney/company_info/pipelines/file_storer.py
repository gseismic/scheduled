import os
import json
from scheduled import Pipeline


class FileStorer(Pipeline):

    def __init__(self, *args, **kwargs):
        Pipeline.__init__(self, *args, **kwargs)
        self.out_root_path = self.config['out_root_path']
        os.makedirs(self.out_root_path, exist_ok=True)

    def process(self, key, data, options):
        if not data:
            return data, options

        filename = os.path.join(self.out_root_path, key+'.json')

        text = json.dumps(options['parser'], ensure_ascii=False)
        with open(filename, 'w', encoding='utf8') as f:
            f.write(text)

        return data, options
