using System;
using System.Threading.Tasks;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;

namespace Raven.Server.Utils
{
    public sealed class ExceptionAggregator
    {
        private readonly RavenLogger _logger;
        private readonly string _errorMsg;
        private readonly ConcurrentSet<Exception> _list = new ConcurrentSet<Exception>();

        public ExceptionAggregator(string errorMsg)
            : this(null, errorMsg)
        {
        }

        public ExceptionAggregator(RavenLogger logger, string errorMsg)
        {
            _logger = logger;
            _errorMsg = errorMsg;
        }

        public void Execute(IDisposable d)
        {
            try
            {
                d?.Dispose();
            }
            catch (Exception e)
            {
                _list.Add(e);
            }
        }

        public void Execute(Action action)
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                _list.Add(e);
            }
        }

        public void Execute(string step, Action action)
        {
            using (DisposeAudit.Step(step))
                Execute(action);
        }

        public void Execute(string step, IDisposable d)
        {
            using (DisposeAudit.Step(step))
                Execute(d);
        }

        public async Task ExecuteAsync(Task task)
        {
            try
            {
                await task;
            }
            catch (Exception e)
            {
                _list.Add(e);
            }
        }

        public async Task ExecuteAsync(ValueTask task)
        {
            try
            {
                await task;
            }
            catch (Exception e)
            {
                _list.Add(e);
            }
        }

        public AggregateException GetAggregateException()
        {
            if (_list.IsEmpty)
                return null;

            return new AggregateException(_errorMsg, _list);
        }

        public void ThrowIfNeeded()
        {
            if (_list.IsEmpty)
                return;

            var aggregateException = GetAggregateException();

            if (_logger != null && _logger.IsInfoEnabled)
                _logger.Info(_errorMsg, aggregateException);

            throw aggregateException;
        }
    }
}
