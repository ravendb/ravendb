using System;
using System.Runtime.CompilerServices;

namespace Sparrow.Logging
{
    public class Logger
    {
        private readonly LoggingSource _parent;
        private readonly string _source;
        private readonly string _logger;
        
        [ThreadStatic] private static LogEntry _logEntry;

        public Logger(LoggingSource parent,string source, string logger)
        {
            _parent = parent;
            _source = source;
            _logger = logger;
        }

        public void Info(string msg, Exception ex = null)
        {
            _logEntry.At = DateTime.UtcNow;
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Information;
            _parent.Log(ref _logEntry);
        }

        public void Operations(string msg, Exception ex = null)
        {
            _logEntry.At = DateTime.Now;
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Operations;
            _parent.Log(ref _logEntry);
        }

        public bool IsInfoEnabled
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _parent.IsInfoEnabled; }
        }

        public bool IsOperationsEnabled
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _parent.IsOperationsEnabled; }
        }
    }
}