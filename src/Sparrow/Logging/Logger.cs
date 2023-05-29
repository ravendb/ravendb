using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Sparrow.Logging
{
    public class Logger
    {
        protected readonly LoggingSource LoggingSource;
        public readonly string Source;
        public readonly string Name;
        protected readonly SwitchLogger Parent;

        [ThreadStatic]
        private static LogEntry _logEntry;

        internal Logger(SwitchLogger parent, string source, string name)
            : this(parent.LoggingSource, source, name)
        {
            Parent = parent;
        }

        public Logger(LoggingSource loggingSource, string source, string name)
        {
            LoggingSource = loggingSource;
            Source = source;
            Name = name;
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

        public void Info(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = Name;
            _logEntry.Message = msg;
            _logEntry.Source = Source;
            _logEntry.Type = LogMode.Information;
            _logEntry.OverrideWriteMode = GetOverrideWriteMode();
            
            LoggingSource.Log(ref _logEntry);
        }

        public Task InfoWithWait(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = Name;
            _logEntry.Message = msg;
            _logEntry.Source = Source;
            _logEntry.Type = LogMode.Information;
            _logEntry.OverrideWriteMode = GetOverrideWriteMode();
            
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            LoggingSource.Log(ref _logEntry, tcs);

            return tcs.Task;
        }

        public void Operations(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = Name;
            _logEntry.Message = msg;
            _logEntry.Source = Source;
            _logEntry.Type = LogMode.Operations;
            _logEntry.OverrideWriteMode = GetOverrideWriteMode();
            LoggingSource.Log(ref _logEntry);
        }

        public Task OperationsWithWait(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = Name;
            _logEntry.Message = msg;
            _logEntry.Source = Source;
            _logEntry.Type = LogMode.Operations;
            _logEntry.OverrideWriteMode = GetOverrideWriteMode();

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            LoggingSource.Log(ref _logEntry, tcs);

            return tcs.Task;
        }

        public virtual bool IsInfoEnabled
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Parent?.IsInfoEnabled ?? LoggingSource.IsInfoEnabled; }
        }

        public virtual bool IsOperationsEnabled
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Parent?.IsOperationsEnabled ?? LoggingSource.IsOperationsEnabled; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static DateTime GetLogDate()
        {
            var now = DateTime.UtcNow;
            if (LoggingSource.UseUtcTime == false)
                now = new DateTime(now.Ticks + LoggingSource.LocalToUtcOffsetInTicks);

            return now;
        }

        protected virtual LogMode? GetOverrideWriteMode()
        {
            return Parent == null || Parent.IsModeOverrode == false ? null : Parent.GetLogMode();
        }
    }
}
