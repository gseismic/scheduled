

# 获取失败
class FetcherError(Exception):
    pass


class PipelineError(Exception):
    pass


# fallback Pipeline 执行失败
class FallbackPipelineError(Exception):
    pass


# 使用代理，不同的Key返回同样的结果
class ProxyCacheError(FetcherError):
    pass
