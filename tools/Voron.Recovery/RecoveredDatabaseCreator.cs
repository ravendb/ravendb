using System;
using System.Globalization;
using System.IO;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Threading;
using Document = Raven.Server.Documents.Document;

namespace Voron.Recovery
{
    public class RecoveredDatabaseCreator : IDisposable
    {
        public static RecoveredDatabaseCreator RecoveredDbTools(DocumentDatabase documentDatabase, string recoverySession, Logger logger) => new RecoveredDatabaseCreator(documentDatabase, recoverySession, logger);

        private readonly DatabaseDestination _databaseDestination;
        private readonly SmugglerResult _results;
        private readonly DocumentsOperationContext _context;
        private readonly DocumentDatabase _database;
        private readonly IDisposable _contextDisposal;
        private readonly Logger _logger;
        private readonly string _logDocId;
        private readonly string _orphanAttachmentsDocIdPrefix;
        private readonly string _orphanCountersDocIdPrefix;
        private readonly Slice _attachmentsSlice;
        private readonly ByteStringContext _byteStringContext;
        private readonly string _recoveryLogCollection;

        internal static string GetOrphanAttachmentDocId(string docName, string hash) => $"{docName}/{hash}";

        private RecoveredDatabaseCreator(DocumentDatabase documentDatabase, string recoverySession, Logger logger)
        {
            _logger = logger;
            _recoveryLogCollection = $"RecoveryLog-{recoverySession}";
            _orphanAttachmentsDocIdPrefix = $"OrphanAttachments/{recoverySession}";
            _orphanCountersDocIdPrefix = $"OrphanCounters/{recoverySession}";
            _logDocId = $"Log/{recoverySession}";
            _database = documentDatabase;
            _databaseDestination = new DatabaseDestination(documentDatabase);
            _results = new SmugglerResult();
            _databaseDestination.Initialize(new DatabaseSmugglerOptionsServerSide(), _results, ServerVersion.Build);
            _contextDisposal = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            _byteStringContext = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(_byteStringContext, "Attachments", ByteStringType.Immutable, out _attachmentsSlice);

            CreateRecoveryLogDocument(recoverySession);
        }

        private void CreateRecoveryLogDocument(string recoverySession)
        {
            using (var tx = _context.OpenWriteTransaction())
            {
                using (var doc = _context.ReadObject(
                    new DynamicJsonValue
                    {
                        ["RecoverySession"] = recoverySession,
                        ["RecoveryStarted"] = DateTime.Now,
                        [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue {[Raven.Client.Constants.Documents.Metadata.Collection] = _recoveryLogCollection}
                    }, _logDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk)
                )
                {
                    _database.DocumentsStorage.Put(_context, _logDocId, null, doc);
                }
                tx.Commit();
            }
        }

        public static void Log(string msg, string logDocId, Logger logger, DocumentsOperationContext context, DocumentDatabase database, Exception ex = null)
        {
            if (logger.IsOperationsEnabled)
                logger.Operations(msg, ex);

            DocumentsTransaction tx = null;
            if (context.HasTransaction == false)
                tx = context.OpenWriteTransaction();
            try
            {
                var logDoc = database.DocumentsStorage.Get(context, logDocId);
                if (ex != null)
                    msg += $". Exception: {ex}";
                logDoc.Data.Modifications = new DynamicJsonValue
                {
                    [DateTime.Now.ToString(CultureInfo.InvariantCulture)] = msg
                };
                var newDocument = context.ReadObject(logDoc.Data, logDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                database.DocumentsStorage.Put(context, logDocId, null, newDocument);
                tx?.Commit();
            }
            finally
            {
                tx?.Dispose();
            }
        }

        public void LogWithException(string msg, Exception ex)
        {
            try
            {
                Log(msg, _logDocId, _logger, _context, _database, ex);
            }
            catch (Exception)
            {
                // ignore
            }
        }

        public void Dispose()
        {
            using (var tx = _context.OpenWriteTransaction())
            {
                var logDoc = _database.DocumentsStorage.Get(_context, _logDocId);
                logDoc.Data.Modifications = new DynamicJsonValue(logDoc.Data) {["RecoveryFinished"] = DateTime.Now};
                var newDocument = _context.ReadObject(logDoc.Data, _logDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                _database.DocumentsStorage.Put(_context, _logDocId, null, newDocument);
                tx.Commit();
            }
            _contextDisposal.Dispose();
            _byteStringContext.Dispose();
        }

        public void WriteAttachment(string hash, string name, string contentType, FileStream attachmentsStream, long totalSize)
        {
            DocumentItemsRecovery.WriteAttachment(hash, name, contentType, attachmentsStream, totalSize,
                _database, _context, _orphanCountersDocIdPrefix, _recoveryLogCollection, _attachmentsSlice,
                _logDocId, _logger);
        }

        public void WriteCounterItem(CounterGroupDetail counterGroup)
        {
            DocumentItemsRecovery.WriteCounterItem(
                counterGroup, _database, _databaseDestination, _orphanCountersDocIdPrefix, _context, _results, _recoveryLogCollection, _attachmentsSlice, _logDocId, _logger);
        }

        public void WriteDocument(Document document)
        {
            DocumentRecovery.WriteDocument(
                document, _database, _databaseDestination, _orphanCountersDocIdPrefix, _context, _results, _recoveryLogCollection, _attachmentsSlice, _logDocId, _logger);
        }

        public void WriteRevision(Document revision)
        {
            DocumentRecovery.WriteRevision(
                revision, _database, _databaseDestination, _orphanCountersDocIdPrefix, _context, _results, _recoveryLogCollection, _attachmentsSlice, _logDocId, _logger);
        }

        public void WriteConflict(DocumentConflict conflict)
        {
            DocumentRecovery.WriteConflict(
                conflict, _databaseDestination, _results);
        }
    }
}
