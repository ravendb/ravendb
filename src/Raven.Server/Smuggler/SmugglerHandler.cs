// -----------------------------------------------------------------------
//  <copyright file="SmugglerHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler
{
    public class SmugglerHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/smuggler/export", "POST")]
        public Task PostExport()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                new DatabaseDataExporter(Database).Export(context, ResponseBodyStream());
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/smuggler/import", "POST")]
        public async Task PostImport()
        {
            // var fileName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("fileName");
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            //TODO: detect gzip or not based on query string param
            using (var stream = new GZipStream(HttpContext.Request.Body, CompressionMode.Decompress))
            {
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(context, state, "fileName"))
                {
                    var buffer = context.GetParsingBuffer();
                    int objectDepth = 0;
                    string operateOnType = null;
                    while (true)
                    {
                        if (parser.Read() == false)
                        {
                            var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                            if (read == 0)
                            {
                                if (state.CurrentTokenType != JsonParserToken.EndObject)
                                    throw new EndOfStreamException("Stream ended without reaching end of json content");
                                break;
                            }
                            parser.SetBuffer(buffer, read);
                            continue;
                        }

                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                            case JsonParserToken.False:
                            case JsonParserToken.True:
                            case JsonParserToken.Float:
                            case JsonParserToken.Integer:
                            case JsonParserToken.Separator:
                            case JsonParserToken.StartArray:
                            case JsonParserToken.EndArray:
                                break;
                            case JsonParserToken.String:
                                if (objectDepth == 1)
                                    unsafe
                                    {
                                        operateOnType = new LazyStringValue(null, state.StringBuffer, state.StringSize, context).ToString();
                                    }
                                break;
                            case JsonParserToken.StartObject:
                                if (objectDepth == 1)
                                {
                                    switch (operateOnType)
                                    {
                                        case "Docs":
                                            using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "f", parser, state))
                                            {
                                                builder.ReadNestedObject();
                                                while (builder.Read() == false)
                                                {
                                                    var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                                                    if (read == 0)
                                                        throw new EndOfStreamException("Stream ended without reaching end of json content");
                                                    parser.SetBuffer(buffer, read);
                                                }
                                                builder.FinalizeDocument();
                                                using (var reader = builder.CreateReader())
                                                /*Use longer write trasactions*/
                                                using (var tx = context.OpenWriteTransaction())
                                                {
                                                    BlittableJsonReaderObject metadata;
                                                    if (reader.TryGet(Constants.Metadata, out metadata) == false)
                                                        throw new InvalidOperationException("A document must have a metadata");
                                                    // We are using the @id here and not @key in order to be backward compatiable with old export files.
                                                    string key;
                                                    if (metadata.TryGet(Constants.MetadataDocId, out key) == false)
                                                        throw new InvalidOperationException("Document's metadata must include the document's key.");
                                                    Database.DocumentsStorage.Put(context, key, null, reader);
                                                    tx.Commit();
                                                }
                                            }
                                            break;
                                        case "Attachments":
                                            /*TODO:Should we warn here or write to log*/
                                            break;
                                        case "Indexes":
                                        case "Transformers":
                                            /*TODO:Implement*/
                                            break;
                                        case "Identities":
                                            /*TODO: should we override identieies values?*/
                                            break;
                                    }
                                }
                                else
                                {
                                    objectDepth++;
                                }
                                break;
                            case JsonParserToken.EndObject:
                                objectDepth--;
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                }
            }
        }
    }
}