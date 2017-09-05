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
        private int _documentsCount;

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
                        _documentsCount++;

                        var document = _database.Factory.FromReader(reader, table, out var attachments);

                        await InsertDocument(document, attachments);
                    }
                }
                
                OnTableWritten?.Invoke(table.Name, (double) sw.ElapsedMilliseconds / 1000);
            }
            await EnqueueCommands();
            Console.WriteLine("Documents count: " + _documentsCount);
        }

        private const int BatchSize = 1000;
        private int _count;
        private readonly BatchRequestParser.CommandData[] _commands = new BatchRequestParser.CommandData[BatchSize];
        private readonly List<AttachmentHandler.MergedPutAttachmentCommand> _attachmentCommands = new List<AttachmentHandler.MergedPutAttachmentCommand>();
        private readonly List<IDisposable> _toDispose = new List<IDisposable>();

        private async Task InsertDocument(RavenDocument ravenDocument, Dictionary<string, byte[]> attachments)
        {
            var document = _context.ReadObject(ravenDocument, ravenDocument.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            _commands[_count++] = new BatchRequestParser.CommandData
            {
                Type = CommandType.PUT,
                Document = document,
                Id = ravenDocument.Id
            };

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

            if (_count % BatchSize == 0)
            {
                await EnqueueCommands();
                _count = 0;
                _attachmentCommands.Clear();
            }
        }

        private async Task EnqueueCommands()
        {
            using (var command = new BatchHandler.MergedBatchCommand { Database = _database.DocumentDatabase })
            {
                command.ParsedCommands = new ArraySegment<BatchRequestParser.CommandData>(_commands, 0, _count);

                try
                {
                    await _database.DocumentDatabase.TxMerger.Enqueue(command);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }

                try
                {
                    foreach (var attachment in _attachmentCommands)
                        await _database.DocumentDatabase.TxMerger.Enqueue(attachment);
                }
                catch (Exception)
                {
                    // TODO: Throw exception
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
        }
    }
}
