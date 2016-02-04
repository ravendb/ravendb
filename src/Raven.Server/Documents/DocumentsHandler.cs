// -----------------------------------------------------------------------
//  <copyright file="GetDocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;

namespace Raven.Server.Documents
{
    public class DocumentsHandler : DatabaseRequestHandler
    {
        [Route("/databases/*/docs", "PUT")]
        public async Task Put()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var ids = HttpContext.Request.Query["id"];
                if (ids.Count == 0)
                    throw new ArgumentException("The 'id' query string parameter is mandatory");

                var id = ids[0];
                if (string.IsNullOrWhiteSpace(id))
                    throw new ArgumentException("The 'id' query string parameter must have a non empty value");

                var doc = await context.ReadForDisk(HttpContext.Request.Body, id);

                long? etag = null;
                var etags = HttpContext.Request.Headers["If-None-Match"];
                if (etags.Count != 0)
                {
                    long result;
                    if (long.TryParse(etags[0], out result) == false)
                        throw new ArgumentException(
                            "Could not parse header 'If-None-Match' header as int64, value was: " + etags[0]);
                    etag = result;
                }

                context.Transaction = context.Environment.WriteTransaction();
                if (id[id.Length - 1] == '/')
                {
                    id = id + DocumentsStorage.IdentityFor(context, id);
                }
                DocumentsStorage.Put(context, id, etag, doc);
                context.Transaction.Commit();

                HttpContext.Response.StatusCode = 201;
                HttpContext.Response.Headers["Location"] = id;
            }
        }

        [Route("/databases/*/queries","GET")]
        public Task Get()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();

                var writer = new BlittableJsonTextWriter(context, HttpContext.Response.Body);
                writer.WriteStartObject();
                writer.WritePropertyName(context.GetLazyStringFor("Results"));
                writer.WriteStartArray();
                var first = true;
                foreach (var id in HttpContext.Request.Query["id"])
                {
                    var result = DocumentsStorage.Get(context, id);
                    if (result == null)
                        continue;
                    if(first == false)
                        writer.WriteComma();
                    first = false;
                    var mutableMetadata = GetMutableMetadata(result);
                    mutableMetadata["@id"] = result.Key;
                    mutableMetadata["@etag"] = result.Etag;

                    result.Data.WriteTo(writer);       
                }
                writer.WriteEndArray();
                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringFor("Includes"));
                writer.WriteStartArray();
                //TODO: Includes
                writer.WriteEndArray();

                writer.WriteEndObject();
                writer.Flush();
            }

            return Task.CompletedTask;
        }

        private static DynamicJsonValue GetMutableMetadata(Document result)
        {
            DynamicJsonValue mutableMetadata;
            BlittableJsonReaderObject metadata;
            if (result.Data.TryGet(Constants.Metadata, out metadata) == false)
            {
                result.Data.Modifications = new DynamicJsonValue(result.Data)
                {
                    [Constants.Metadata] = mutableMetadata = new DynamicJsonValue()
                };
            }
            else
            {
                metadata.Modifications = mutableMetadata = new DynamicJsonValue(metadata);
            }
            return mutableMetadata;
        }
    }
}