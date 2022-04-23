class PseudoThreadPoolExecutor:
    '''
    Avoid using real thread pools when there are no synchronous tasks, this can improve performance.
    '''
    def __init__(self, *args, **kwargs):
        ...

    def __enter__(self):
        return None 

    def __exit__(self, *args, **kwargs):
        return False