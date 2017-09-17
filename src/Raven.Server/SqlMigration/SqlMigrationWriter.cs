using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using CommandType = Raven.Client.Documents.Commands.Batches.CommandType;

namespace Raven.Server.SqlMigration
{
    public delegate void DocumentsInsertedEventHandler(double time, int rowsRead, int documentsInserted, int attachmentsInserted);

    internal class SqlMigrationWriter : IDisposable
    {
        public event DocumentsInsertedEventHandler OnDocumentsInserted;

        private int _rowsRead;

        private readonly DocumentsOperationContext _context;
        private readonly SqlDatabase _database;
        private BatchHandler.MergedBatchCommand _command;

        private readonly Stopwatch _time;

        public SqlMigrationWriter(DocumentsOperationContext context, SqlDatabase database)
        {
            if (database.IsValid() == false)
                throw new InvalidOperationException("Database is not valid");

            _database = database;
            _context = context;

            NewCommand();

            _rowsRead = 0;

            _time = new Stopwatch();
        }

        public async Task WriteDatabase()
        {
            SqlReader.RowsRead = 0;
            _time.Start();

            foreach (var table in _database.Tables)
            {
                if (table.IsEmbedded)
                    continue;

                using (var reader = table.GetReader())
                {
                    while (reader.Read())
                    {
                        var migrationDocument = _database.Factory.FromReader(reader, table, out var attachments);

                        await InsertDocument(migrationDocument.ToBllitable(_context), migrationDocument.Id, attachments);
                    }
                }
            }
            await FlushCommands();
        }

        private readonly List<BatchRequestParser.CommandData> _commands = new List<BatchRequestParser.CommandData>();
        private readonly List<BatchHandler.MergedBatchCommand.AttachmentStream> _attachmentStreams = new List<BatchHandler.MergedBatchCommand.AttachmentStream>();
        private readonly List<IDisposable> _toDispose = new List<IDisposable>();
        private async Task InsertDocument(BlittableJsonReaderObject document, string id, Dictionary<string, byte[]> attachments)
        {
            _commands.Add(new BatchRequestParser.CommandData
            {
                Type = CommandType.PUT,
                Document = document,
                Id = id
            });

            foreach (var attachment in attachments)
            {
                var memoryStream = new MemoryStream(attachment.Value);
                var stream = _command.AttachmentStreamsTempFile.StartNewStream();

                _toDispose.Add(memoryStream);
                _toDispose.Add(stream);

                var attachmentStream = new BatchHandler.MergedBatchCommand.AttachmentStream
                {
                    Stream = stream,
                    Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(_context, memoryStream, stream, _context.DocumentDatabase.DatabaseShutdown)
                };

                _attachmentStreams.Add(attachmentStream);

                _commands.Add(new BatchRequestParser.CommandData
                {
                    Type = CommandType.AttachmentPUT,
                    Id = id,
                    ContentType = "",
                    Name = attachment.Key
                });
            }

            if (_commands.Count >= _database.Factory.Options.BatchSize)
                await FlushCommands();
        }
  
        private async Task FlushCommands()
        {
            _command.ParsedCommands = new ArraySegment<BatchRequestParser.CommandData>(_commands.ToArray(), 0, _commands.Count);
            _command.AttachmentStreams = new Queue<BatchHandler.MergedBatchCommand.AttachmentStream>(_attachmentStreams);
           
            try
            {
                await _context.DocumentDatabase.TxMerger.Enqueue(_command);
                
            }
            catch (Exception e)
            {
                var ids = string.Join(", ", _command.ParsedCommands.Select(cmd => cmd.Id));
                throw new InvalidOperationException($"Failed to enqueue batch of {_commands.Count} documents and attachments. ids: {ids}", e);
            }

            var attachmentsCount = _command.ParsedCommands.Count(x => x.Type == CommandType.AttachmentPUT);

            OnDocumentsInserted?.Invoke((double) _time.ElapsedMilliseconds / 1000, SqlReader.RowsRead - _rowsRead, _command.ParsedCommands.Count - attachmentsCount, attachmentsCount);
            _rowsRead = SqlReader.RowsRead;
            _time.Restart();

            NewCommand();

            _commands.Clear();
            _attachmentStreams.Clear();

            foreach (var dispose in _toDispose)
                dispose.Dispose();

            _toDispose.Clear();
        }

        private void NewCommand()
        {
            _command?.Dispose();
            _command = new BatchHandler.MergedBatchCommand
            {
                Database = _context.DocumentDatabase,
                AttachmentStreamsTempFile = _context.DocumentDatabase.DocumentsStorage.AttachmentsStorage.GetTempFile("put")
            };
        }

        public void Dispose()
        {
            SqlReader.DisposeAll();
            SqlConnection.ClearAllPools();
            _command.Dispose();
        }
    }
}
