using System;
using System.Collections.Generic;

namespace Raven.Server.Utils
{
    public class DisposeableScope : IDisposable
    {
        private readonly LinkedList<IDisposable> _disposables = new LinkedList<IDisposable>();
        private int _delayedDispose;

        public void EnsureDispose(IDisposable toDispose)
        {
            _disposables.AddFirst(toDispose);
        }

        public void Dispose()
        {
            if (_delayedDispose-- > 0)
                return;

            List<Exception> errors = null; 

            foreach (var disposable in _disposables)
            {
                try
                {
                    disposable.Dispose();
                }
                catch (Exception e)
                {
                    if (errors == null)
                        errors = new List<Exception>();
                    
                    errors.Add(e);
                }
            }

            if (errors != null)
                throw new AggregateException(errors);
        }

        public IDisposable Delay()
        {
            _delayedDispose++;

            return this;
        }
    }
}