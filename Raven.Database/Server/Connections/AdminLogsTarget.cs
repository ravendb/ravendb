//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;

using Raven.Abstractions.Logging;
using Raven.Database.Server.Controllers;

namespace Raven.Database.Server.Connections
{
    public class AdminLogsTarget : Target 
    {
        private readonly ILog logger = LogManager.GetCurrentClassLogger();

        // used for removing never used configurations
        readonly TimeSensitiveStore<string> timeSensitiveStore = new TimeSensitiveStore<string>(TimeSpan.FromSeconds(45));

        readonly ConcurrentDictionary<string, AdminLogsConnectionState> connections = new ConcurrentDictionary<string, AdminLogsConnectionState>();


        public TimeSensitiveStore<string> TimeSensitiveStore
        {
            get { return timeSensitiveStore; }
        }

        private void AlterEnabled()
        {
            LogManager.EnableDebugLogForTargets = connections.Count > 0;
        }

        public void OnIdle()
        {
            timeSensitiveStore.ForAllExpired(s =>
            {
                AdminLogsConnectionState value;
                if (connections.TryRemove(s, out value))
                    value.Dispose();
            });
            AlterEnabled();
        }

        public void Disconnect(string id)
        {
            timeSensitiveStore.Seen(id);
            AdminLogsConnectionState value;
            if (connections.TryRemove(id, out value))
                value.Dispose();
            AlterEnabled();
        }

        public AdminLogsConnectionState Register(IEventsTransport transport)
        {
            timeSensitiveStore.Seen(transport.Id);
            transport.Disconnected += () =>
            {
                timeSensitiveStore.Missing(transport.Id);
                AdminLogsConnectionState _;
                connections.TryRemove(transport.Id, out _);
                AlterEnabled();
            };
            return connections.AddOrUpdate(
                transport.Id,
                new AdminLogsConnectionState(transport),
                (s, state) =>
                {
                    state.Reconnect(transport);
                    return state;
                });
        }

        public override void Write(LogEventInfo logEvent)
        {
            if (connections.Count > 0)
            {
                foreach (var connection in connections)
                {
                    connection.Value.Send(logEvent);
                }
            }
        }

        public override bool ShouldLog(ILog log, LogLevel level)
        {
            return LogManager.EnableDebugLogForTargets;
        }

        public AdminLogsConnectionState For(string id, RavenBaseApiController controller = null)
        {
            var connection = connections.GetOrAdd(
                id,
                _ =>
                {
                    IEventsTransport logsTransport = null;
                    if (controller != null)
                        logsTransport = new LogsPushContent(controller);

                    var connectionState = new AdminLogsConnectionState(logsTransport);
                    TimeSensitiveStore.Missing(id);
                    return connectionState;
                });

            AlterEnabled();
            return connection;
        }

        public override void Dispose()
        {
            foreach (var connectionState in connections)
            {
                try
                {
                    connectionState.Value.Dispose();
                }
                catch (Exception e)
                {
                    logger.InfoException("Could not disconnect transport connection", e);
                }
            }    
            AlterEnabled();
        }
    }
}
