using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Microsoft.AspNet.Http;
using Raven.Server.Json.Parsing;

namespace Raven.Server.Documents
{
    public class BatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_docs", "POST")]
        public async Task BulkdDocs()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                BlittableJsonReaderArray commands;
                try
                {
                    // TODO : implement depth to skip (change also var nanme)
                    commands = await context.ParseArrayToMemory(RequestBodyStream(), "bulk/docs",
                        BlittableJsonDocumentBuilder.UsageMode.ToDisk, 3);
                }
                catch (InvalidDataException)
                {
                    throw;
                }
                catch (Exception ioe)
                {
                    throw new InvalidDataException("Could not parse json", ioe);
                }

                var reply = new DynamicJsonArray();

                using (context.Transaction = context.Environment.WriteTransaction())
                {
                    for (int i = 0; i < commands.Count; i++)
                    {
                        var cmd = commands.GetByIndex<BlittableJsonReaderObject>(i);

                        string method;
                        if (cmd.TryGet("Method", out method) == false)
                            throw new InvalidDataException("Missing 'Method' property");
                        string key;
                        if (cmd.TryGet("Key", out key) == false)
                            throw new InvalidDataException("Missing 'Key' property");
                        long? etag;
                        cmd.TryGet("ETag", out etag);
                        BlittableJsonReaderObject additionalData;
                        cmd.TryGet("AdditionalData", out additionalData);

                        switch (method)
                        {
                            case "PUT":
                                var newEtag = DocumentsStorage.Put(context, key, etag, cmd);

                                reply.Add(new DynamicJsonValue
                                {
                                    ["Key"] = key,
                                    ["Etag"] = newEtag,
                                    ["Method"] = method,
                                    ["AdditionalData"] = additionalData
                                });
                                break;

                            case "DELETE":
                                var deleted =  DocumentsStorage.Delete(context, key, etag);
                                reply.Add(new DynamicJsonValue
                                {
                                    ["Key"] = key,
                                    ["Etag"] = etag,
                                    ["Method"] = method,
                                    ["AdditionalData"] = additionalData,
                                    ["Deleted"] = deleted
                                });
                                break;
                        }
                    }

                    context.Transaction.Commit();
                }

                HttpContext.Response.StatusCode = 201;

                var writer = new BlittableJsonTextWriter(context, ResponseBodyStream());
                await context.WriteAsync(writer, reply);
                writer.Flush();
            }
        }
    }
}

      