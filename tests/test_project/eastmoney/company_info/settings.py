KEY_YIELDER = 'key_yielder.KeyYielder'
FETCHER = 'fetcher.Fetcher'
PIPELINES = [
    'pipelines.parser.Parser', 
    'pipelines.file_storer.FileStorer', 
    'pipelines.sqlite_storer.SqliteStorer'
]

FALLBACK_PIPELINES = []
