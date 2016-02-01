// -----------------------------------------------------------------------
//  <copyright file="GetDocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Raven.Server.Web;
using RequestHandler = Raven.Server.Web.RequestHandler;

namespace Raven.Server.Documents
{
    public class DocumentsHandler : RequestHandler
    {
        private readonly RequestHandlerContext _context;

        public DocumentsHandler(RequestHandlerContext context)
        {
            _context = context;
        }

        [Route("/databases/*/docs", "PUT")]
        public Task Put()
        {
            RavenOperationContext context;
            using (_context.OperationContextPool.AllocateOperationContext(out context))
            {
                var ids = _context.HttpContext.Request.Query["id"];
                if (ids.Count == 0)
                    throw new ArgumentException("The 'id' query string parameter is mandatory");

                var id = ids[0];
                if (string.IsNullOrWhiteSpace(id))
                    throw new ArgumentException("The 'id' query string parameter must have a non empty value");

                var doc = context.ReadForDisk(_context.HttpContext.Request.Body, id);

                long? etag = null;
                var etags = _context.HttpContext.Request.Headers["If-None-Match"];
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
                    id = id + _context.DocumentStore.IdentityFor(context, id);
                }
                _context.DocumentStore.Put(context, id, etag, doc);
                context.Transaction.Commit();

                _context.HttpContext.Response.StatusCode = 201;
                _context.HttpContext.Response.Headers["Location"] = id;
            }
            return Task.CompletedTask;
        }

        [Route("/databases/*/queries","GET")]
        public Task Get()
        {
            RavenOperationContext context;
            using (_context.OperationContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();

                var writer = new BlittableJsonTextWriter(context, _context.HttpContext.Response.Body);
                writer.WriteStartObject();
                writer.WritePropertyName(context.GetLazyStringFor("Results"));
                writer.WriteStartArray();
                var first = true;
                foreach (var id in _context.HttpContext.Request.Query["id"])
                {
                    var result = _context.DocumentStore.Get(context, id);
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