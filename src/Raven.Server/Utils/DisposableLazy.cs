using System;

namespace Raven.Server.Utils
{
    public class DisposableLazy<T> : IDisposable where T : IDisposable
    {
        private readonly Lazy<T> _lazy;

        public DisposableLazy(Func<T> factory)
        {
            _lazy = new Lazy<T>(factory);
        }

        public T Value => _lazy.Value;

        public bool IsValueCreated => _lazy.IsValueCreated;

        public void Dispose()
        {
            if (_lazy.IsValueCreated)
                _lazy.Value.Dispose();
        }
    }
}
