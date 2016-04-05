using System;
using Raven.Abstractions.Logging;
using Sparrow.Collections;

namespace Raven.Server.Utils
{
    public class ExceptionAggregator
    {
        private readonly ILog _log;
        private readonly string _errorMsg;
        private readonly LogLevel _level;
        private readonly ConcurrentSet<Exception> _list = new ConcurrentSet<Exception>();

        public ExceptionAggregator(string errorMsg)
            : this(null, errorMsg)
        {
        }

        public ExceptionAggregator(ILog log, string errorMsg, LogLevel level = LogLevel.Error)
        {
            _log = log;
            _errorMsg = errorMsg;
            _level = level;
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

        public void ThrowIfNeeded()
        {
            if (_list.Count == 0)
                return;

            var aggregateException = new AggregateException(_errorMsg, _list);

            _log?.Log(_level, () => _errorMsg, aggregateException);

            throw aggregateException;
        }
    }
}
