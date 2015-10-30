using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;

namespace Raven.Database.Server.Connections
{
    public class AdminLogsConnectionState : IDisposable
    {
        private readonly EasyReaderWriterLock slim = new EasyReaderWriterLock();

        private readonly SortedDictionary<string, LogConfig> logConfig = new SortedDictionary<string, LogConfig>(new ReverseStringComparer());

        private class LogConfig
        {
            public LogLevel Level;
            public bool WatchStack;
        }

        private IEventsTransport logsTransport;

        public AdminLogsConnectionState(IEventsTransport logsTransport)
        {
            this.logsTransport = logsTransport;
        }

        public void Reconnect(IEventsTransport transport)
        {
            logsTransport = transport;
        }

        public object DebugStatus
        {
            get
            {
                using (slim.EnterReadLock())
                {
                    return new SortedDictionary<string, LogConfig>(logConfig, new ReverseStringComparer()); 
                }
            }
        }


        public void EnableLogging(string category, LogLevel minLevel, bool watchStack)
        {
            using (slim.EnterWriteLock())
            {
                logConfig[category] = new LogConfig
                {
                    Level = minLevel,
                    WatchStack = watchStack
                };
            }
        }

        public bool DisableLogging(string category)
        {
            using (slim.EnterWriteLock())
            {
                return logConfig.Remove(category);
            }
        }

        public void Send(LogEventInfo logEvent)
        {
            if (logsTransport == null || logsTransport.Connected == false)
            {
                return;
            }
            bool shouldLog = false;
            bool watchStackTrace = false;
            using (slim.EnterReadLock())
            {
                foreach (var config in logConfig)
                {
                    if(logEvent.LoggerName.StartsWith(config.Key) == false)
                        continue;
                    if(logEvent.Level < config.Value.Level)
                        continue;
                    shouldLog = true;
                    watchStackTrace = config.Value.WatchStack;
                }
            }
            if (shouldLog == false) 
                return;
            if(watchStackTrace)
                logEvent.StackTrace = new StackTrace();
            logsTransport.SendAsync(logEvent);
        }

        public void Dispose()
        {
            if (logsTransport != null)
                logsTransport.Dispose();
        }
    }

    class ReverseStringComparer : IComparer<string>
    {
        private readonly StringComparer comparer = StringComparer.InvariantCulture;

        public int Compare(string x, string y)
        {
            return comparer.Compare(x, y) * -1;
        }
    }
}
