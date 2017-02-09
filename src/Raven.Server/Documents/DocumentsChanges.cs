using System;
using System.Collections.Concurrent;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Server.Documents.Expiration;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class DocumentsChanges
    {
        
        public readonly ConcurrentDictionary<long, ChangesClientConnection> Connections = new ConcurrentDictionary<long, ChangesClientConnection>();
        private Logger _logger;

        public event Action<DocumentChange> OnSystemDocumentChange;

        public event Action<DocumentChange> OnDocumentChange;

        public event Action<IndexChange> OnIndexChange;

        public event Action<TransformerChange> OnTransformerChange;

        public event Action<OperationStatusChanged> OnOperationStatusChange;

        public DocumentsChanges(string databaseName)
        {
            _logger = LoggingSource.Instance.GetLogger<DocumentChange>(databaseName);
        }
        public void RaiseNotifications(IndexChange indexChange)
        {
            OnIndexChange?.Invoke(indexChange);

            foreach (var connection in Connections)
            {
                try
                {
                    if (connection.Value.IsDisposed == false)
                        connection.Value.SendIndexChanges(indexChange);
                }
                catch (Exception ex)
                {
                    _logger.Operations("Failed to notify about index change",ex);
                }
            }
        }

        public void RaiseNotifications(TransformerChange transformerChange)
        {
            OnTransformerChange?.Invoke(transformerChange);

            foreach (var connection in Connections)
            {
                try
                {
                    if (connection.Value.IsDisposed==false)
                        connection.Value.SendTransformerChanges(transformerChange);
                }
                catch (Exception ex)
                {
                    _logger.Operations("Failed to notify about transformer change", ex);
                }
            }
        }

        public void RaiseSystemNotifications(DocumentChange documentChange)
        {
            var k = OnSystemDocumentChange;
            if (k != null)
            {
                var invocationList = k.GetInvocationList().GroupBy(x => x.Target)
                    .Where(x => x.Count() > 1)
                    .ToArray();

                foreach (var grouping in invocationList)
                {
                    Console.WriteLine(grouping.Key + " " + grouping.Count());
                }

            }
            OnSystemDocumentChange?.Invoke(documentChange);

            foreach (var connection in Connections)
            {
                try
                {
                    if (connection.Value.IsDisposed)
                        connection.Value.SendDocumentChanges(documentChange);
                }
                catch (Exception ex)
                {
                    _logger.Operations("Failed to notify about system document change", ex);
                }
            }
        }

        public void RaiseNotifications(DocumentChange documentChange)
        {
            OnDocumentChange?.Invoke(documentChange);

            foreach (var connection in Connections)
            {
                try
                {
                    if (connection.Value.IsDisposed == false)
                        connection.Value.SendDocumentChanges(documentChange);
                }
                catch (Exception ex)
                {
                    _logger.Operations("Failed to notify about document change", ex);
                }
            }
                
        }

        public void RaiseNotifications(OperationStatusChanged operationStatusChange)
        {
            OnOperationStatusChange?.Invoke(operationStatusChange);

            foreach (var connection in Connections)
            {

                try
                {
                    if (connection.Value.IsDisposed==false)
                        connection.Value.SendOperationStatusChangeNotification(operationStatusChange);
                }
                catch (Exception ex)
                {
                    _logger.Operations("Failed to notify about operation status change", ex);

                }
            }
        }

        public void Connect(ChangesClientConnection connection)
        {
            Connections.TryAdd(connection.Id, connection);
        }

        public void Disconnect(long id)
        {
            ChangesClientConnection connection;
            if (Connections.TryRemove(id, out connection))
                connection.Dispose();
        }
    }
}