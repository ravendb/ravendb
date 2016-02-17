// -----------------------------------------------------------------------
//  <copyright file="BulkInsertHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Handlers
{
    public class BulkInsertHandler : DatabaseRequestHandler
    {
        [Routing.RavenAction("/databases/*/bulkInsert", "POST")]
        public async Task BulkInsert()
        {
            if (HttpContext.Request.Query["op"] == "generate-single-use-auth-token")
            {
                // using windows auth with anonymous access = none sometimes generate a 401 even though we made two requests
                // instead of relying on windows auth, which require request buffering, we generate a one time token and return it.
                // we KNOW that the user have access to this db for writing, since they got here, so there is no issue in generating 
                // a single use token for them.

                // TODO: generate API tokens
                // TODO: look at Context.HttpContext.Authentication.ChallengeAsync()
                //var authorizer = (MixedModeRequestAuthorizer)Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

                //var token = authorizer.GenerateSingleUseAuthToken(DatabaseName, User);
                //return GetMessageWithObject(new
                //{
                //    Token = token
                //});
                return;
            }

            //var options = new BulkInsertOptions
            //{
            //    OverwriteExisting = GetBooleanQueryStringOption("overwriteExisting"),
            //    CheckReferencesInIndexes = GetBooleanQueryStringOption("checkReferencesInIndexes"),
            //    //TODO: complete
            //};

            //TODO: handle timeouts


            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var buffer = new byte[1024];
                string input = null;


                var receiveAsync =
                    await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);

                input = Encoding.UTF8.GetString(buffer, 0, receiveAsync.Count);
                Console.WriteLine(input);
                var size = Encoding.UTF8.GetBytes(input.Reverse().ToArray(), 0, input.Length, buffer, 0);
                await
                    webSocket.SendAsync(new ArraySegment<byte>(buffer, 0, size), WebSocketMessageType.Text, true,
                        CancellationToken.None);

                var batch = new List<Document>();
                var requestBody = HttpContext.Request.Body;
                var reader = new BinaryReader(requestBody);

                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    while (true)
                    {
                        int compressedBatchSize;
                        try
                        {
                            compressedBatchSize = reader.ReadInt32(); // not required
                        }
                        catch (EndOfStreamException)
                        {
                            break;
                        }


                        using (var batchStream = new PartialStream(requestBody, compressedBatchSize))
                        //TODO: We need to figure out a way to reuse this stream, creating it each time means big allocations
                        using (var stream = new GZipStream(batchStream, CompressionMode.Decompress, leaveOpen: true))
                        {
                            var batchReader = new BinaryReader(stream);
                            var count = batchReader.ReadInt32();
                            foreach (
                                var doc in
                                    await context.ParseMultipleDocuments(stream, count,
                                        BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                            {
                                BlittableJsonReaderObject metadata;
                                string id;
                                if (doc.TryGet(Constants.Metadata, out metadata) == false ||
                                    metadata.TryGet("@id", out id) == false ||
                                    string.IsNullOrEmpty(id))
                                {
                                    throw new InvalidDataException("Could not get id from document metadata");
                                }
                                if (id.Equals(Constants.BulkImportHeartbeatDocKey, StringComparison.OrdinalIgnoreCase))
                                {
                                    //its just a token document, should not get written into the database
                                    //the purpose of the heartbeat document is to make sure that the connection doesn't time-out
                                    //during long pauses in the bulk insert operation.
                                    // Currently used by smuggler to make sure that the connection doesn't time out if there is a 
                                    //continuation token and lots of document skips
                                    continue;
                                }

                                batch.Add(new Document
                                {
                                    Key = LazyStringFromKey(id, context),
                                    Data = doc
                                });
                            }
                        }


                        if (batch.Count > 0)
                        {
                            using (context.OpenWriteTransaction())
                            {
                                foreach (var doc in batch)
                                {
                                    Database.DocumentsStorage.Put(context, doc.Key, null, doc.Data);
                                }

                                context.Transaction.Commit();
                            }
                        }

                        batch.Clear();
                        context.Reset();
                    }
                }

                await webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "BulkInsert Finished", CancellationToken.None);
            }
            return;
        }

        private unsafe LazyStringValue LazyStringFromKey(string id, MemoryOperationContext context)
        {
            return new LazyStringValue(id, null, 0, context);
        }

        private bool GetBooleanQueryStringOption(string name)
        {
            bool result;
            if (bool.TryParse(HttpContext.Request.Query[name], out result))
                return result;
            return false;
        }


        public class BulkInsertStatus //TODO: implements operations state : IOperationState
        {
            public int Documents { get; set; }
            public bool Completed { get; set; }

            public bool Faulted { get; set; }

            //TODO: report state
            //public RavenJToken State { get; set; }

            public bool IsTimedOut { get; set; }

            public bool IsSerializationError { get; set; }
        }
    }
}