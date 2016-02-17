// -----------------------------------------------------------------------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class ChangesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/changes", "GET", "/databases/{databaseName:string}/changes")]
        public async Task GetChangesEvents()
        {
            var debugTag = "changes/" + Database.Name;

            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                // TODO: catch a disconnected clients and deregister them

                var connection = new NotificationsClientConnection(webSocket, Database);
                Database.Notifications.Connect(connection);

                RavenOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    var buffer = context.GetManagedBuffer();
                    var receiveAsync = webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), Database.DatabaseShutdown);
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
                    {
                        var result = await receiveAsync;
                        receiveAsync = webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), Database.DatabaseShutdown);
                        parser.SetBuffer(buffer, result.Count);

                        while (true)
                        {
                            using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState))
                            {
                                builder.ReadObject();
                                
                                while (builder.Read() == false)
                                {
                                    result = await receiveAsync;
                                    receiveAsync = webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), Database.DatabaseShutdown);
                                    parser.SetBuffer(buffer, result.Count);
                                }

                                builder.FinalizeDocument();

                                var reader = builder.CreateReader();
                                string command, commandParameter;
                                if (reader.TryGet("Command", out command) == false)
                                {
                                    // Write error
                                    throw new NotImplementedException();
                                    // TBD: what should be done here
                                }

                                reader.TryGet("Param", out commandParameter);

                                if (Match(command, "disconnect"))
                                {
                                    Database.Notifications.Disconnect(connection);
                                    break;
                                }
                                HandelCommand(connection, command, commandParameter);
                            }
                        }
                    }
                }
            }
        }

        private void HandelCommand(NotificationsClientConnection connection, string command, string commandParameter)
        {
            /* if (Match(command, "watch-index"))
             {
                 connection.WatchIndex(commandParameter);
             }
             else if (Match(command, "unwatch-index"))
             {
                 connection.UnwatchIndex(commandParameter);
             }
             else if (Match(command, "watch-indexes"))
             {
                 connection.WatchAllIndexes();
             }
             else if (Match(command, "unwatch-indexes"))
             {
                 connection.UnwatchAllIndexes();
             }
             else if (Match(command, "watch-transformers"))
             {
                 connection.WatchTransformers();
             }
             else if (Match(command, "unwatch-transformers"))
             {
                 connection.UnwatchTransformers();
             }
             else*/
            if (Match(command, "watch-doc"))
            {
                connection.WatchDocument(commandParameter);
            }
            else if (Match(command, "unwatch-doc"))
            {
                connection.UnwatchDocument(commandParameter);
            }
            else if (Match(command, "watch-docs"))
            {
                connection.WatchAllDocuments();
            }
            else if (Match(command, "unwatch-docs"))
            {
                connection.UnwatchAllDocuments();
            }
            else if (Match(command, "watch-prefix"))
            {
                connection.WatchDocumentPrefix(commandParameter);
            }
            else if (Equals(command, "unwatch-prefix"))
            {
                connection.UnwatchDocumentPrefix(commandParameter);
            }
            else if (Match(command, "watch-collection"))
            {
                connection.WatchDocumentInCollection(commandParameter);
            }
            else if (Equals(command, "unwatch-collection"))
            {
                connection.UnwatchDocumentInCollection(commandParameter);
            }
            else if (Match(command, "watch-type"))
            {
                connection.WatchDocumentOfType(commandParameter);
            }
            else if (Equals(command, "unwatch-type"))
            {
                connection.UnwatchDocumentOfType(commandParameter);
            }
            /*else if (Match(command, "watch-replication-conflicts"))
            {
                connection.WatchAllReplicationConflicts();
            }
            else if (Match(command, "unwatch-replication-conflicts"))
            {
                connection.UnwatchAllReplicationConflicts();
            }
            else if (Match(command, "watch-bulk-operation"))
            {
                connection.WatchBulkInsert(commandParameter);
            }
            else if (Match(command, "unwatch-bulk-operation"))
            {
                connection.UnwatchBulkInsert(commandParameter);
            }
            else if (Match(command, "watch-data-subscriptions"))
            {
                connection.WatchAllDataSubscriptions();
            }
            else if (Match(command, "unwatch-data-subscriptions"))
            {
                connection.UnwatchAllDataSubscriptions();
            }
            else if (Match(command, "watch-data-subscription"))
            {
                connection.WatchDataSubscription(long.Parse(commandParameter));
            }
            else if (Match(command, "unwatch-data-subscription"))
            {
                connection.UnwatchDataSubscription(long.Parse(commandParameter));
            }*/
            else
            {
                throw new NotImplementedException();
                /*return GetMessageWithObject(new
                {
                    Error = "command argument is mandatory"
                }, HttpStatusCode.BadRequest);*/
            }
        }

        protected static bool Match(string x, string y)
        {
            return string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
        }
    }
}