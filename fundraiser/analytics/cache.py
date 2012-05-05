import functools
import cPickle

def cache(func):
    stored_results = {}

    @functools.wraps(func)
    def cached(*args, **kwargs):
        key = cPickle.dumps((args, sorted(kwargs.iteritems())))
        try:
            # try to get the cached result
            return stored_results[key]
        except KeyError:
            # nothing was cached for those args. let's fix that.
            result = stored_results[key] = func(*args, **kwargs)
            return result

    return cached