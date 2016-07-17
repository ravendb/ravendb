// -----------------------------------------------------------------------
//  <copyright file="SmugglerHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
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
                    string operateOnType = null;
                    var batchPutCommand = new MergedBatchPutCommand(Database);
                    var identities = new Dictionary<string, long>();
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
                            case JsonParserToken.String:
                                unsafe
                                {
                                    operateOnType = new LazyStringValue(null, state.StringBuffer, state.StringSize, context).ToString();
                                }
                                break;
                            case JsonParserToken.StartObject:
                                switch (operateOnType)
                                {
                                    case "Docs":
                                        var documentBuilder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "f", parser, state);
                                        documentBuilder.ReadNestedObject();
                                        while (documentBuilder.Read() == false)
                                        {
                                            var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                                            if (read == 0)
                                                throw new EndOfStreamException("Stream ended without reaching end of json content");
                                            parser.SetBuffer(buffer, read);
                                        }
                                        documentBuilder.FinalizeDocument();
                                        batchPutCommand.Add(documentBuilder);
                                        if (batchPutCommand.Count >= 16)
                                        {
                                            await Database.TxMerger.Enqueue(batchPutCommand);
                                            batchPutCommand.Dispose();
                                            batchPutCommand = new MergedBatchPutCommand(Database);
                                        }
                                        break;
                                    case "Attachments":
                                        /*TODO:Should we warn here or write to log*/
                                        break;
                                    case "Indexes":
                                    case "Transformers":
                                        using (var indexesBuilder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "Indexes", parser, state))
                                        {
                                            indexesBuilder.ReadNestedObject();
                                            while (indexesBuilder.Read() == false)
                                            {
                                                var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                                                if (read == 0)
                                                    throw new EndOfStreamException("Stream ended without reaching end of json content");
                                                parser.SetBuffer(buffer, read);
                                            }
                                            indexesBuilder.FinalizeDocument();
                                            using (var reader = indexesBuilder.CreateReader())
                                            {
                                                /*TODO:Implement*/
                                            }
                                        }
                                        break;
                                    case "Identities":
                                        using (var identitiesBuilder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "Identities", parser, state))
                                        {
                                            identitiesBuilder.ReadNestedObject();
                                            while (identitiesBuilder.Read() == false)
                                            {
                                                var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                                                if (read == 0)
                                                    throw new EndOfStreamException("Stream ended without reaching end of json content");
                                                parser.SetBuffer(buffer, read);
                                            }
                                            identitiesBuilder.FinalizeDocument();
                                            using (var reader = identitiesBuilder.CreateReader())
                                            {
                                                string identityKey, identityValueString;
                                                if (reader.TryGet("Key", out identityKey) == false)
                                                {
                                                    // TODO: Warn
                                                }
                                                if (reader.TryGet("Value", out identityValueString) == false)
                                                {
                                                    // TODO: Warn
                                                }
                                                long identityValue;
                                                if (long.TryParse(identityValueString, out identityValue) == false)
                                                {
                                                    // TODO: Warn
                                                }
                                                identities[identityKey] = identityValue;
                                            }
                                        }
                                        break;
                                    default:
                                        /*TODO:Should we warn here or write to log*/
                                        break;
                                }
                                break;
                            case JsonParserToken.EndArray:
                                switch (operateOnType)
                                {
                                    case "Docs":
                                        if (batchPutCommand.Count > 0)
                                            await Database.TxMerger.Enqueue(batchPutCommand);
                                        batchPutCommand = null;
                                        break;
                                    case "Identities":
                                        using (var tx = context.OpenWriteTransaction())
                                        {
                                            Database.DocumentsStorage.UpdateIdentities(context, identities);
                                            tx.Commit();
                                        }
                                        identities = null;
                                        break;
                                }
                                break;
                        }
                    }
                }
            }
        }

        private class MergedBatchPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            private readonly DocumentDatabase _database;
            private readonly List<IDisposable> _buildersToDispose = new List<IDisposable>();
            private readonly List<BlittableJsonReaderObject> _documents = new List<BlittableJsonReaderObject>();

            public MergedBatchPutCommand(DocumentDatabase database)
            {
                _database = database;
            }

            public int Count
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return _documents.Count; }
            }

            public override void Execute(DocumentsOperationContext context, RavenTransaction tx)
            {
                foreach (var document in _documents)
                {
                    BlittableJsonReaderObject metadata;
                    if (document.TryGet(Constants.Metadata, out metadata) == false)
                        throw new InvalidOperationException("A document must have a metadata");
                    // We are using the id term here and not key in order to be backward compatiable with old export files.
                    string key;
                    if (metadata.TryGet(Constants.MetadataDocId, out key) == false)
                        throw new InvalidOperationException("Document's metadata must include the document's key.");
                    _database.DocumentsStorage.Put(context, key, null, document);
                }
            }

            public void Dispose()
            {
                foreach (var documentBuilder in _buildersToDispose)
                {
                    documentBuilder.Dispose();
                }
                foreach (var documentBuilder in _documents)
                {
                    documentBuilder.Dispose();
                }
            }

            public void Add(BlittableJsonDocumentBuilder documentBuilder)
            {
                _buildersToDispose.Add(documentBuilder);
                var reader = documentBuilder.CreateReader();
                _documents.Add(reader);
            }
        }
    }
}