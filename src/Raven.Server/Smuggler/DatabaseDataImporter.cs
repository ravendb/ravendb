using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler
{
    public class DatabaseDataImporter
    {
        private readonly DocumentDatabase _database;

        public DatabaseDataImporter(DocumentDatabase database)
        {
            _database = database;
        }

        public async Task<ImportResult> Import(DocumentsOperationContext context, Stream stream)
        {
            var result = new ImportResult();

            var state = new JsonParserState();
            using (var parser = new UnmanagedJsonParser(context, state, "fileName"))
            {
                var buffer = context.GetParsingBuffer();
                string operateOnType = null;
                var batchPutCommand = new MergedBatchPutCommand(_database);
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
                                    result.DocumentsCount++;
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
                                        await _database.TxMerger.Enqueue(batchPutCommand);
                                        batchPutCommand.Dispose();
                                        batchPutCommand = new MergedBatchPutCommand(_database);
                                    }
                                    break;
                                case "Attachments":
                                    result.Warnings.Add("Attachments are not supported anymore. Use RavenFS isntead. Skipping.");
                                    break;
                                case "Indexes":
                                case "Transformers":
                                    /*result.IndexesCount++;
                                    result.TransformersCount++;*/
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
                                    result.IdentitiesCount++;
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
                                            try
                                            {
                                                string identityKey, identityValueString;
                                                long identityValue;
                                                if (reader.TryGet("Key", out identityKey) == false ||
                                                    reader.TryGet("Value", out identityValueString) == false ||
                                                    long.TryParse(identityValueString, out identityValue) == false)
                                                {
                                                    result.Warnings.Add($"Cannot import the following identity: '{reader}'. Skipping.");
                                                }
                                                else
                                                {
                                                    identities[identityKey] = identityValue;
                                                }
                                            }
                                            catch (Exception e)
                                            {
                                                result.Warnings.Add($"Cannot import the following identity: '{reader}'. Error: {e}. Skipping.");
                                            }
                                        }
                                    }
                                    break;
                                default:
                                    result.Warnings.Add($"The following type is not recognized: '{operateOnType}'. Skipping.");
                                    using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "Identities", parser, state))
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
                                    }
                                    break;
                            }
                            break;
                        case JsonParserToken.EndArray:
                            switch (operateOnType)
                            {
                                case "Docs":
                                    if (batchPutCommand.Count > 0)
                                        await _database.TxMerger.Enqueue(batchPutCommand);
                                    batchPutCommand = null;
                                    break;
                                case "Identities":
                                    using (var tx = context.OpenWriteTransaction())
                                    {
                                        _database.DocumentsStorage.UpdateIdentities(context, identities);
                                        tx.Commit();
                                    }
                                    identities = null;
                                    break;
                            }
                            break;
                    }
                }
            }

            return result;
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