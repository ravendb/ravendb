using System;

namespace Sparrow.Utils
{
    public class LazyWithExceptionRetry<T>
    {
        private Lazy<T> _inner;

        private Func<T> _factory;

        public LazyWithExceptionRetry(Func<T> factory)
        {
            _factory = factory;
            _inner = new Lazy<T>(factory);
        }

        private bool _faulted;

        public bool IsValueFaulted => _faulted;
        public bool IsValueCreated => _inner.IsValueCreated;

        public T Value
        {
            get
            {
                try
                {
                    T value = _inner.Value;
                    _faulted = false;
                    _factory = null;
                    return value;
                }
                catch
                {
                    if (_factory != null) 
                    {
                        _faulted = true;
                        _inner = new Lazy<T>(_factory);
                    }
                    throw;
                }
            }
        }


    }
}
