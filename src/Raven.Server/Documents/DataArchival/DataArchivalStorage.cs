using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Sparrow.Json.Parsing;
using Voron.Impl;
using Bits = Sparrow.Binary.Bits;

namespace Raven.Server.Documents.DataArchival
{
    public unsafe class DataArchivalStorage
    {
        private const string DocumentsByArchiveAtDateTime = "DocumentsByArchiveAtDateTime";
        
        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;
        private readonly Logger _logger;

        public DataArchivalStorage(DocumentDatabase database, Transaction tx)
        {
            _database = database;
            _documentsStorage = _database.DocumentsStorage;
            _logger = LoggingSource.Instance.GetLogger<DataArchivalStorage>(database.Name);

            tx.CreateTree(DocumentsByArchiveAtDateTime);
        }

        public void Put(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject metadata)
        {
            var hasArchiveDate = metadata.TryGet(Constants.Documents.Metadata.ArchiveAt, out string archiveDate);
            if (hasArchiveDate == false)
                return;

            PutInternal(context, lowerId, archiveDate);
        }

        private void PutInternal(DocumentsOperationContext context, Slice lowerId, string archiveDate)
        {
            if (DateTime.TryParseExact(archiveDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime date) == false) 
                ThrowWrongArchiveDateFormat(lowerId, archiveDate);

            var archives = date.ToUniversalTime();
            var ticksBigEndian = Bits.SwapBytes(archives.Ticks);

            var tree = context.Transaction.InnerTransaction.ReadTree(DocumentsByArchiveAtDateTime);
            using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
                tree.MultiAdd(ticksSlice, lowerId);
        }
        
        [DoesNotReturn]
        private void ThrowWrongArchiveDateFormat(Slice lowerId, string archiveDate)
        {
            throw new InvalidOperationException(
                $"The archive date format for document '{lowerId}' is not valid: '{archiveDate}'. Use the following format: {_database.Time.GetUtcNow():O}");
        }

        public record ArchivedDocumentsOptions
        {
            public DocumentsOperationContext Context;
            public DateTime CurrentTime;
            public DatabaseTopology Topology;
            public string NodeTag;
            public long AmountToTake;
            
            public ArchivedDocumentsOptions(DocumentsOperationContext context, DateTime currentTime, DatabaseTopology topology, string nodeTag, long amountToTake) =>
                (Context, CurrentTime, Topology, NodeTag, AmountToTake)
                = (context, currentTime, topology, nodeTag, amountToTake);
        }

        public Dictionary<Slice, List<(Slice LowerId, string Id)>> GetDocumentsToArchive(ArchivedDocumentsOptions options, out Stopwatch duration,
            CancellationToken cancellationToken)
        {
            return GetDocuments(options, out duration, cancellationToken);
        }

        private Dictionary<Slice, List<(Slice lowerId, string Id)>> GetDocuments(ArchivedDocumentsOptions options, out Stopwatch duration,
            CancellationToken cancellationToken)
        {
            var count = 0;
            var currentTicks = options.CurrentTime.Ticks;

            var documentsByArchiveDateTree = options.Context.Transaction.InnerTransaction.ReadTree(DocumentsByArchiveAtDateTime);
            using (var it = documentsByArchiveDateTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                {
                    duration = null;
                    return null;
                }

                var toArchive = new Dictionary<Slice, List<(Slice LowerId, string Id)>>();
                duration = Stopwatch.StartNew();

                do
                {
                    var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                    if (entryTicks > currentTicks)
                        break;

                    var ticksAsSlice = it.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                    var docsToArchive = new List<(Slice LowerId, string Id)>();

                    using (var multiIt = documentsByArchiveDateTree.MultiRead(it.CurrentKey))
                    {
                        if (multiIt.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                if (cancellationToken.IsCancellationRequested)
                                    return toArchive;

                                var clonedId = multiIt.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                                using (var document = _database.DocumentsStorage.Get(options.Context, clonedId,
                                           DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector))
                                {
                                    if (document is null ||
                                        document.TryGetMetadata(out var metadata) == false ||
                                        BackgroundWorkHelper.HasPassed(metadata, options.CurrentTime, Constants.Documents.Metadata.ArchiveAt) == false)
                                    {
                                        docsToArchive.Add((clonedId, null));
                                        continue;
                                    }

                                    if (BackgroundWorkHelper.CheckIfNodeIsFirstInTopology(options) == false)
                                        break;
                                    
                                    docsToArchive.Add((clonedId, document.Id));
                                }
                            } while (multiIt.MoveNext() && docsToArchive.Count + count < options.AmountToTake);
                        }
                    }

                    count += docsToArchive.Count;
                    if (docsToArchive.Count > 0)
                        toArchive.Add(ticksAsSlice,docsToArchive);
                    
                    
                } while (it.MoveNext() && count < options.AmountToTake);

                return toArchive;
            }
        }

        private bool ArchiveDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
        {
            if (id == null)
                throw new InvalidOperationException($"Couldn't archive the document. Document id is null. Lower id is {lowerId}");

            using (var doc = _database.DocumentsStorage.Get(context, lowerId, DocumentFields.Data, throwOnConflict: true))
            {
                if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                {
                    throw new InvalidOperationException($"Failed to fetch the metadata of document '{id}'");
                }
                
                if (BackgroundWorkHelper.HasPassed(metadata, currentTime, Constants.Documents.Metadata.ArchiveAt) == false) 
                    return false;

                // Add archived flag, remove archive timestamp, add document flag
                metadata.Modifications = new DynamicJsonValue(metadata);
                metadata.Modifications[Constants.Documents.Metadata.Archived] = true;
                metadata.Modifications.Remove(Constants.Documents.Metadata.ArchiveAt);
                doc.Flags |= DocumentFlags.Archived;
                

                using (var updated = context.ReadObject(doc.Data, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _database.DocumentsStorage.Put(context, id, null, updated, flags: doc.Flags.Strip(DocumentFlags.FromClusterTransaction),
                        nonPersistentFlags: NonPersistentDocumentFlags.SkipRevisionCreation);
                }
            }

            return true;
        }
        
        public int ArchiveDocuments(DocumentsOperationContext context, Dictionary<Slice, List<(Slice LowerId, string Id)>> toArchive, DateTime currentTime)
        {
            int archivedCount = 0;
            var archiveTree = context.Transaction.InnerTransaction.ReadTree(DocumentsByArchiveAtDateTime);

            foreach (var pair in toArchive)
            {
                foreach (var ids in pair.Value)
                {
                    bool timePassed = ArchiveDocument(context, ids.LowerId, ids.Id, currentTime);
                    
                    if (timePassed == false) 
                        continue;
                    
                    archiveTree.MultiDelete(pair.Key, ids.LowerId);
                    archivedCount++;
                }
            }
            return archivedCount;
        }
    }
}
