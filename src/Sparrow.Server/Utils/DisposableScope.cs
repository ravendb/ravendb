using System;
using System.Collections.Generic;

namespace Sparrow.Server.Utils
{
    public sealed class DisposableScope : IDisposable
    {
        private readonly Stack<IDisposable> _disposables = new();
        private int _delayedDispose;

        public void EnsureDispose(IDisposable toDispose)
        {
            _disposables.Push(toDispose);
        }

        public void Dispose()
        {
            if (_delayedDispose-- > 0)
                return;

            List<Exception> errors = null;
            while (_disposables.TryPop(out var disposable))
            {
                try
                {
                    disposable.Dispose();
                }
                catch (Exception e)
                {
                    errors ??= new List<Exception>();
                    errors.Add(e);
                }
            }

            if (errors != null)
                throw new AggregateException(errors);
        }

        /// <summary>
        /// Delays the disposal and provides the scope that should be disposed first.
        /// </summary>
        /// <returns>An additional disposable scope.</returns>
        public IDisposable Delay()
        {
            _delayedDispose++;
            return this;
        }
    }
}
