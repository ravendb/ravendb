using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Sparrow.Logging
{
    public class Logger
    {
        private readonly LoggingSource _parent;
        private readonly string _source;
        private readonly string _logger;

        [ThreadStatic]
        private static LogEntry _logEntry;

        public Logger(LoggingSource parent, string source, string logger)
        {
            _parent = parent;
            _source = source;
            _logger = logger;
        }

        public void Info(FormattableString msg, Exception e = null)
        {
            try
            {
                var msgStr = msg.ToString();
                Info(msgStr, e);
            }
            catch (Exception)
            {
                Info(msg.Format, e);
            }
        }

        public void Operations(FormattableString msg, Exception e = null)
        {
            try
            {
                var msgStr = msg.ToString();
                Operations(msgStr, e);
            }
            catch (Exception)
            {
                Info(msg.Format, e);
            }
        }

        public void Info(string msg, Exception ex = null, bool track = false)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Information;
            _parent.Log(ref _logEntry, track: track);
        }

        public Task InfoAsync(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Information;

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            _parent.Log(ref _logEntry, tcs);

            return tcs.Task;
        }

        public void Operations(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Operations;
            _parent.Log(ref _logEntry);
        }

        public Task OperationsAsync(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Operations;

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            _parent.Log(ref _logEntry, tcs);

            return tcs.Task;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static DateTime GetLogDate()
        {
            var now = DateTime.UtcNow;
            if (LoggingSource.UseUtcTime == false)
                now = new DateTime(now.Ticks + LoggingSource.LocalToUtcOffsetInTicks);

            return now;
        }
    }
}
