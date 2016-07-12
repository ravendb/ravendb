using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents.Handlers
{
    public class BatchHandler : DatabaseRequestHandler
    {
        private struct CommandData
        {
            public string Method;
            public string Key;
            public BlittableJsonReaderObject Document;
            public PatchRequest Patch;
            public BlittableJsonReaderObject AdditionalData;
            public long? Etag;
            public bool IsDebugMode;
        }

        [RavenAction("/databases/*/bulk_docs", "POST")]
        public async Task BulkDocs()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                BlittableJsonReaderArray commands;
                try
                {
                    commands = await context.ParseArrayToMemoryAsync(RequestBodyStream(), "bulk/docs",
                        // we will prepare the docs to disk in the actual PUT command
                        BlittableJsonDocumentBuilder.UsageMode.None);
                }
                catch (InvalidDataException)
                {
                    throw;
                }
                catch (Exception ioe)
                {
                    throw new InvalidDataException("Could not parse json", ioe);
                }

                CommandData[] parsedCommands = new CommandData[commands.Length];

                for (int i = 0; i < commands.Length; i++)
                {
                    var cmd = commands.GetByIndex<BlittableJsonReaderObject>(i);

                    if (cmd.TryGet("Method", out parsedCommands[i].Method) == false)
                        throw new InvalidDataException("Missing 'Method' property");
                    if (cmd.TryGet("Key", out parsedCommands[i].Key) == false)
                        throw new InvalidDataException("Missing 'Key' property");

                    // optional
                    cmd.TryGet("ETag", out parsedCommands[i].Etag);
                    cmd.TryGet("AdditionalData", out parsedCommands[i].AdditionalData);

                    // We have to do additional processing on the documents
                    // in particular, prepare them for disk by compressing strings, validating floats, etc

                    // We **HAVE** to do that outside of the write transaction lock, that is why we are handling
                    // it in this manner, first parse the commands, then prepare for the put, finally open
                    // the transaction and actually write
                    switch (parsedCommands[i].Method)
                    {
                        case "PUT":
                            BlittableJsonReaderObject doc;
                            if (cmd.TryGet("Document", out doc) == false)
                                throw new InvalidDataException("Missing 'Document' property");

                            // we need to split this document to an independent blittable document
                            // and this time, we'll prepare it for disk.
                            doc.PrepareForStorage();
                            parsedCommands[i].Document = context.ReadObject(doc, parsedCommands[i].Key,
                                BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            break;
                        case "PATCH":
                            cmd.TryGet("DebugMode", out parsedCommands[i].IsDebugMode);

                            BlittableJsonReaderObject patch;
                            if (cmd.TryGet("Patch", out patch) == false)
                                throw new InvalidDataException("Missing 'Patch' property");

                            parsedCommands[i].Patch = PatchRequest.Parse(patch);
                            break;
                    }
                }

                var mergedCmd = new MergedBatchCommand
                {
                    Database = Database,
                    ParsedCommands = parsedCommands,
                    Reply = new DynamicJsonArray()
                };

                await Database.TxMerger.Enqueue(mergedCmd);

                HttpContext.Response.StatusCode = 201;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.Write(writer, mergedCmd.Reply);
            }
        }

        private class MergedBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public DynamicJsonArray Reply;
            public CommandData[] ParsedCommands;
            public DocumentDatabase Database;

            public override void Execute(DocumentsOperationContext context, RavenTransaction tx)
            {
                for (int i = 0; i < ParsedCommands.Length; i++)
                {
                    var cmd = ParsedCommands[i];
                    switch (cmd.Method)
                    {
                        case "PUT":
                            var putResult = Database.DocumentsStorage.Put(context, cmd.Key, cmd.Etag,
                                cmd.Document);

                            BlittableJsonReaderObject metadata;
                            cmd.Document.TryGet(Constants.Metadata, out metadata);

                            Reply.Add(new DynamicJsonValue
                            {
                                ["Key"] = putResult.Key,
                                ["Etag"] = putResult.ETag,
                                ["Method"] = "PUT",
                                ["AdditionalData"] = cmd.AdditionalData,
                                ["Metadata"] = metadata
                            });
                            break;
                        case "PATCH":
                            // TODO: Move this code out of the merged transaction
                            // TODO: We should have an object that handles this externally, 
                            // TODO: and apply it there
                            var patchResult = Database.Patch.Apply(context, cmd.Key, cmd.Etag, cmd.Patch, null, cmd.IsDebugMode);
                            var additionalData = new DynamicJsonValue
                            {
                                ["Debug"] = patchResult.DebugInfo,
                            };
                            if (cmd.IsDebugMode)
                            {
                                additionalData["Document"] = patchResult.ModifiedDocument;
                                additionalData["Actions"] = patchResult.DebugActions;
                            }
                            Reply.Add(new DynamicJsonValue
                            {
                                ["Key"] = cmd.Key,
                                ["Etag"] = cmd.Etag,
                                ["Method"] = "PATCH",
                                ["AdditionalData"] = additionalData,
                                ["PatchResult"] = patchResult.PatchResult.ToString(),
                            });
                            break;
                        case "DELETE":
                            var deleted = Database.DocumentsStorage.Delete(context, cmd.Key, cmd.Etag);
                            Reply.Add(new DynamicJsonValue
                            {
                                ["Key"] = cmd.Key,
                                ["Method"] = "DELETE",
                                ["AdditionalData"] = cmd.AdditionalData,
                                ["Deleted"] = deleted
                            });
                            break;
                    }
                }

            }
        }
    }
}

