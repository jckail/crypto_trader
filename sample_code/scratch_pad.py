def retry_if_io_error(exception):
    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
    x = isinstance(exception, requests.exceptions.RequestException)
    print x

retry_if_io_error(requests.exceptions.RequestException)