// -----------------------------------------------------------------------
//  <copyright file="TransportState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Server.Controllers;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.Server.Connections
{
    public class TransportState : IDisposable
    {
        private static readonly ILog logger = LogManager.GetCurrentClassLogger();

        readonly TimeSensitiveStore<string> timeSensitiveStore = new TimeSensitiveStore<string>(TimeSpan.FromSeconds(45));

        readonly ConcurrentDictionary<string, ConnectionState> connections = new ConcurrentDictionary<string, ConnectionState>();

        public TimeSensitiveStore<string> TimeSensitiveStore
        {
            get { return timeSensitiveStore; }
        }

        public void OnIdle()
        {
            timeSensitiveStore.ForAllExpired(s =>
            {
                ConnectionState value;
                if(connections.TryRemove(s, out value))
                    value.Dispose();
            });
        }

        public void Disconnect(string id)
        {
            timeSensitiveStore.Seen(id);
            ConnectionState value;
            if(connections.TryRemove(id, out value))
                value.Dispose();
        }

        public ConnectionState Register(IEventsTransport transport)
        {
            timeSensitiveStore.Seen(transport.Id);
            transport.Disconnected += () => Disconnect(transport.Id);

            return connections.AddOrUpdate(transport.Id, new ConnectionState(transport), (s, state) =>
                                                                                            {
                                                                                                state.Reconnect(transport);
                                                                                                return state;
                                                                                            });
        }

        public event Action<object, IndexChangeNotification> OnIndexChangeNotification = delegate { }; 

        public void Send(IndexChangeNotification indexChangeNotification)
        {
            OnIndexChangeNotification(this, indexChangeNotification);
            foreach (var connectionState in connections)
            {
                connectionState.Value.Send(indexChangeNotification);
            }
        }

        public event Action<object, TransformerChangeNotification> OnTransformerChangeNotification = delegate { }; 

        public void Send(TransformerChangeNotification transformerChangeNotification)
        {
            OnTransformerChangeNotification(this, transformerChangeNotification);
            foreach (var connectionState in connections)
            {
                connectionState.Value.Send(transformerChangeNotification);
            }
        }

        public event Action<object, DocumentChangeNotification> OnDocumentChangeNotification = delegate { }; 

        public void Send(DocumentChangeNotification documentChangeNotification)
        {
            OnDocumentChangeNotification(this, documentChangeNotification);
            foreach (var connectionState in connections)
            {
                connectionState.Value.Send(documentChangeNotification);
            }
        }

        public event Action<object, Notification> OnNotification = delegate { };

        public void Send(Notification notification)
        {
            OnNotification(this, notification);
            foreach (var connectionState in connections)
            {
                connectionState.Value.Send(notification);
            }
        }

        public event Action<object, BulkInsertChangeNotification> OnBulkInsertChangeNotification = delegate { };

        public void Send(BulkInsertChangeNotification bulkInsertChangeNotification)
        {
            OnBulkInsertChangeNotification(this, bulkInsertChangeNotification);
            foreach (var connectionState in connections)
            {
                connectionState.Value.Send(bulkInsertChangeNotification);
            }
        }

        public event Action<object, ReplicationConflictNotification> OnReplicationConflictNotification = delegate { };

        public void Send(ReplicationConflictNotification replicationConflictNotification)
        {
            OnReplicationConflictNotification(this, replicationConflictNotification);
            foreach (var connectionState in connections)
            {
                connectionState.Value.Send(replicationConflictNotification);
            }
        }

        public event Action<object, DataSubscriptionChangeNotification> OnDataSubscriptionChangeNotification = delegate { };

        public void Send(DataSubscriptionChangeNotification dataSubscriptionChangeNotification)
        {
            OnDataSubscriptionChangeNotification(this, dataSubscriptionChangeNotification);
            foreach (var connectionState in connections)
            {
                connectionState.Value.Send(dataSubscriptionChangeNotification);
            }
        }

        public ConnectionState For(string id, RavenBaseApiController controller = null)
        {
            return connections.GetOrAdd(id, _ =>
            {
                IEventsTransport eventsTransport = null;
                if (controller != null)
                    eventsTransport = new ChangesPushContent(controller);
                
                var connectionState = new ConnectionState(eventsTransport);
                TimeSensitiveStore.Missing(id);
                return connectionState;
            });
        }

        public object[] DebugStatuses
        {
            get { return connections.Values.Select(x=>x.DebugStatus).ToArray(); }
        }

        public void Dispose()
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
        }
    }
}
