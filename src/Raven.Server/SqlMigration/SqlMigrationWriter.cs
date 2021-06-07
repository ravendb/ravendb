using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using CommandType = Raven.Client.Documents.Commands.Batches.CommandType;

namespace Raven.Server.SqlMigration
{
    internal class SqlMigrationWriter : IDisposable
    {
        private readonly DocumentsOperationContext _context;
        private BatchHandler.MergedBatchCommand _command;
        private readonly int _batchSize;

        public SqlMigrationWriter(DocumentsOperationContext context, int batchSize)
        {
            _context = context;
            _batchSize = batchSize;

            Reset();
        }

        private readonly List<BatchRequestParser.CommandData> _commands = new List<BatchRequestParser.CommandData>();
        private readonly List<BatchHandler.MergedBatchCommand.AttachmentStream> _attachmentStreams = new List<BatchHandler.MergedBatchCommand.AttachmentStream>();
        private readonly List<IDisposable> _toDispose = new List<IDisposable>();

        public async Task InsertDocument(BlittableJsonReaderObject document, string id, Dictionary<string, byte[]> attachments)
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
                    Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(_context, memoryStream, stream, _context.DocumentDatabase.DatabaseShutdown) //TODO: do we need it?
                };

                await stream.FlushAsync();

                _attachmentStreams.Add(attachmentStream);

                _commands.Add(new BatchRequestParser.CommandData
                {
                    Type = CommandType.AttachmentPUT,
                    Id = id,
                    ContentType = "",
                    Name = attachment.Key
                });
            }

            if (_commands.Count >= _batchSize)
                await FlushCommands();
        }

        private async Task FlushCommands()
        {
            if (_commands.Count == 0 && _attachmentStreams.Count == 0)
                return;

            _command.ParsedCommands = new ArraySegment<BatchRequestParser.CommandData>(_commands.ToArray(), 0, _commands.Count);
            _command.AttachmentStreams = new List<BatchHandler.MergedBatchCommand.AttachmentStream>(_attachmentStreams);

            try
            {
                await _context.DocumentDatabase.TxMerger.Enqueue(_command);
            }
            catch (Exception e)
            {
                var ids = string.Join(", ", _command.ParsedCommands.Select(cmd => cmd.Id));
                throw new InvalidOperationException($"Failed to enqueue batch of {_commands.Count} documents and attachments. ids: {ids}", e);
            }

            Reset();

            foreach (var dispose in _toDispose)
                dispose.Dispose();

            _toDispose.Clear();
        }

        private void Reset()
        {
            _command?.Dispose();
            _command = new BatchHandler.MergedBatchCommand(_context.DocumentDatabase)
            {
                AttachmentStreamsTempFile = _context.DocumentDatabase.DocumentsStorage.AttachmentsStorage.GetTempFile("put")
            };

            _commands.Clear();
            _attachmentStreams.Clear();
        }

        public void Dispose()
        {
            AsyncHelpers.RunSync(FlushCommands);
            _command?.Dispose();
        }
    }
}
