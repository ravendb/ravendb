using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;

namespace Raven.Database.Server.Connections
{
    public class OnDemandLogConnectionState
    {
        private readonly EasyReaderWriterLock slim = new EasyReaderWriterLock();

        private readonly SortedDictionary<string, LogLevel> logConfig = new SortedDictionary<string, LogLevel>(new ReverseStringComparer());

        private ILogsTransport logsTransport;

        public OnDemandLogConnectionState(ILogsTransport logsTransport)
        {
            this.logsTransport = logsTransport;
        }

        public void Reconnect(ILogsTransport transport)
        {
            logsTransport = transport;
        }

        public object DebugStatus
        {
            get
            {
                using (slim.EnterReadLock())
                {
                    return logConfig.ToDictionary(x => x.Key, x => x.Value);    
                }
            }
        }


        private bool ShouldLog(LogEventInfo logEvent)
        {
            using (slim.EnterReadLock())
            {
                return
                    logConfig.Any(
                        categoryAndLevel =>
                        logEvent.LoggerName.StartsWith(categoryAndLevel.Key) && logEvent.Level >= categoryAndLevel.Value);
            }
        }

        public void EnableLogging(string category, LogLevel minLevel)
        {
            using (slim.EnterWriteLock())
            {
                logConfig[category] = minLevel;
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
            if (ShouldLog(logEvent))
            {
                logsTransport.SendAsync(logEvent);    
            }
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