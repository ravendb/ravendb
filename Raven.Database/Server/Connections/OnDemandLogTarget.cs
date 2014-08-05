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
	public class OnDemandLogTarget : Target 
	{
        private readonly ILog logger = LogManager.GetCurrentClassLogger();

        // used for removing never used configurations
        readonly TimeSensitiveStore<string> timeSensitiveStore = new TimeSensitiveStore<string>(TimeSpan.FromSeconds(45));

        readonly ConcurrentDictionary<string, OnDemandLogConnectionState> connections = new ConcurrentDictionary<string, OnDemandLogConnectionState>();

        private bool enabled; // true when connections.Count > 0 - it allows us to avoid locks

        public TimeSensitiveStore<string> TimeSensitiveStore
        {
            get { return timeSensitiveStore; }
        }

        private void AlterEnabled()
        {
            enabled = connections.Count > 0;
        }

        public void OnIdle()
        {
            timeSensitiveStore.ForAllExpired(s =>
            {
                OnDemandLogConnectionState value;
                if (connections.TryRemove(s, out value))
                    value.Dispose();
            });
            AlterEnabled();
        }

        public void Disconnect(string id)
        {
            timeSensitiveStore.Seen(id);
            OnDemandLogConnectionState value;
            if (connections.TryRemove(id, out value))
                value.Dispose();
            AlterEnabled();
        }

        public OnDemandLogConnectionState Register(ILogsTransport transport)
        {
            timeSensitiveStore.Seen(transport.Id);
            transport.Disconnected += () =>
            {
                timeSensitiveStore.Missing(transport.Id);
                OnDemandLogConnectionState _;
                connections.TryRemove(transport.Id, out _);
                AlterEnabled();
            };
            return connections.AddOrUpdate(
                transport.Id,
                new OnDemandLogConnectionState(transport),
                (s, state) =>
                {
                    state.Reconnect(transport);
                    return state;
                });
        }

	    public override void Write(LogEventInfo logEvent)
		{
			if (!logEvent.LoggerName.StartsWith("Raven.")) 
				return;
	        if (connections.Count > 0)
	        {
                foreach (var onDemandLogConfig in connections)
                {
                    onDemandLogConfig.Value.Send(logEvent);
                }
	        }
		}

	    public override bool ShouldLog(ILog logger, LogLevel level)
	    {
	        return enabled;
	    }

        public OnDemandLogConnectionState For(string id, RavenBaseApiController controller = null)
        {
            var connection = connections.GetOrAdd(
                id,
                _ =>
                {
                    ILogsTransport logsTransport = null;
                    if (controller != null)
                        logsTransport = new LogsPushContent(controller);

                    var connectionState = new OnDemandLogConnectionState(logsTransport);
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