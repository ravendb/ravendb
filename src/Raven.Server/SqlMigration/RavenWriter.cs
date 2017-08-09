using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using CommandType = Raven.Client.Documents.Commands.Batches.CommandType;

namespace Raven.Server.SqlMigration
{
    public delegate void TableWrittenEventHandler(string tableName, double time);

    internal class RavenWriter : IDisposable
    {
        public event TableWrittenEventHandler OnTableWritten;

        private readonly DocumentsOperationContext _context;
        private readonly SqlDatabase _database;

        public RavenWriter(DocumentsOperationContext context, SqlDatabase database)
        {
            if (!database.IsValid())
                throw new InvalidOperationException("Database is not valid");

            _database = database;
            _context = context;
        }

        public async Task WriteDatabase()
        {
            var sw = new Stopwatch();

            foreach (var table in _database.Tables)
            {
                if (table.IsEmbedded)
                    continue;

                sw.Restart();

                using (var reader = table.GetReader())
                {
                    while (reader.Read())
                    {
                        var document = _database.Factory.FromReader(reader, table, out var attachments);

                        try
                        {
                            await InsertDocument(document, attachments);
                        }
                        catch (Exception)
                        {
                            if (_database.Factory.Options.SkipUnsopportedTypes)
                                continue;

                            await EnqueueCommands(); // TODO: Should insert the commands in the queue? or fail immediately 
                            throw;
                        }
                    }
                }
                
                OnTableWritten?.Invoke(table.Name, (double) sw.ElapsedMilliseconds / 1000);
            }
            await EnqueueCommands();
        }

        private const int BatchSize = 1000; // TODO:  what's the most efficient batch size?
        private readonly List<BatchRequestParser.CommandData> _documentCommands = new List<BatchRequestParser.CommandData>();
        private readonly List<AttachmentHandler.MergedPutAttachmentCommand> _attachmentCommands = new List<AttachmentHandler.MergedPutAttachmentCommand>();
        private readonly List<IDisposable> _toDispose = new List<IDisposable>();

        private async Task InsertDocument(RavenDocument ravenDocument, Dictionary<string, byte[]> attachments)
        {
            BlittableJsonReaderObject document;

            try
            {
                document = _context.ReadObject(ravenDocument, ravenDocument.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot build raven document from table '{ravenDocument.TableName}'. Raven document id is: {ravenDocument.Id}", e);
            }

            _documentCommands.Add(new BatchRequestParser.CommandData
            {
                Type = CommandType.PUT,
                Document = document,
                Id = ravenDocument.Id
            });

            foreach (var attachment in attachments)
            {
                var streamsTempFile = _database.DocumentDatabase.DocumentsStorage.AttachmentsStorage.GetTempFile("put");
                var stream = streamsTempFile.StartNewStream();
                
                var ms = new MemoryStream(attachment.Value);
                var hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(_context, ms, stream, _database.DocumentDatabase.DatabaseShutdown);

                _attachmentCommands.Add(new AttachmentHandler.MergedPutAttachmentCommand
                {
                    Database = _database.DocumentDatabase,
                    DocumentId = ravenDocument.Id,
                    Name = attachment.Key,
                    Stream = stream,
                    Hash = hash,
                    ContentType = ""
                });
                _toDispose.Add(stream);
                _toDispose.Add(streamsTempFile);
             }

            if (_documentCommands.Count % BatchSize == 0)
            {
                await EnqueueCommands();
                _documentCommands.Clear();
                _attachmentCommands.Clear();
            }
        }

        private async Task EnqueueCommands()
        {
            using (var command = new BatchHandler.MergedBatchCommand { Database = _database.DocumentDatabase })
            {
                command.ParsedCommands = new ArraySegment<BatchRequestParser.CommandData>(_documentCommands.ToArray(), 0, _documentCommands.Count);

                try
                {
                    await _database.DocumentDatabase.TxMerger.Enqueue(command);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to enqueue batch of {_documentCommands.Count} documents", e);
                }

                try
                {
                    foreach (var attachment in _attachmentCommands)
                        await _database.DocumentDatabase.TxMerger.Enqueue(attachment);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to enqueue batch of {_attachmentCommands.Count} attachments", e);
                }
                finally
                {
                    foreach (var disposable in _toDispose)
                    {
                        disposable.Dispose();
                    }
                    _toDispose.Clear();
                }
            }
        }

        public void Dispose()
        {
            SqlReader.DisposeAll();
            SqlConnection.ClearAllPools();
        }
    }
}
