using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client;
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
        private const string DocumentsByArchiveDateTime = "DocumentsByArchiveDateTime";
        
        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;
        private readonly Logger _logger;

        public DataArchivalStorage(DocumentDatabase database, Transaction tx)
        {
            _database = database;
            _documentsStorage = _database.DocumentsStorage;
            _logger = LoggingSource.Instance.GetLogger<DataArchivalStorage>(database.Name);

            tx.CreateTree(DocumentsByArchiveDateTime);
        }

        public void Put(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject metadata)
        {
            var hasArchiveDate = metadata.TryGet(Constants.Documents.Metadata.Archive, out string archiveDate);
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

            var tree = context.Transaction.InnerTransaction.ReadTree(DocumentsByArchiveDateTime);
            using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
                tree.MultiAdd(ticksSlice, lowerId);
        }

        private void ThrowWrongArchiveDateFormat(Slice lowerId, string archiveDate)
        {
            throw new InvalidOperationException(
                $"The archive date format for document '{lowerId}' is not valid: '{archiveDate}'. Use the following format: {_database.Time.GetUtcNow():O}");
        }

        public record ArchivedDocumentsOptions
        {
            public DocumentsOperationContext Context;
            public DateTime CurrentTime;
            public int AmountToTake;
            
            public ArchivedDocumentsOptions(DocumentsOperationContext context, DateTime currentTime, int amoutToTake) =>
                (Context, CurrentTime, AmountToTake)
                = (context, currentTime, amoutToTake);
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

            var documentsByArchiveDateTree = options.Context.Transaction.InnerTransaction.ReadTree(DocumentsByArchiveDateTime);
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
                                    if (document is null || document.TryGetMetadata(out var metadata) == false || HasPassed(metadata, options.CurrentTime) == false)
                                    {
                                        docsToArchive.Add((clonedId, null));
                                        continue;
                                    }
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
        
        private static bool HasPassed(BlittableJsonReaderObject metadata, DateTime currentTime)
        {
            
            if (metadata.TryGet(Constants.Documents.Metadata.Archive, out LazyStringValue archiveDate))
            {
                if (LazyStringParser.TryParseDateTime(archiveDate.Buffer, archiveDate.Length, out DateTime date, out _, properlyParseThreeDigitsMilliseconds: true) == LazyStringParser.Result.DateTime)
                {
                    if (date.Kind != DateTimeKind.Utc) 
                        date = date.ToUniversalTime();
                    
                    if (currentTime >= date)
                        return true;
                }
            }
            return false;
        }

        private static BlittableJsonReaderObject BuildMergedCounterGroupValues(JsonOperationContext context, List<BlittableJsonReaderObject> counterGroupsValues, string docId)
        {
            BlittableJsonReaderObject mergedCounterGroupValues;
            using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                builder.StartWriteObjectDocument();

                builder.StartWriteObject();

                builder.WritePropertyName(CountersStorage.DbIds);
                builder.StartWriteArray();
                
                var firstCounterGroupValues = counterGroupsValues[0];
                if(firstCounterGroupValues.TryGet(CountersStorage.DbIds, out BlittableJsonReaderArray sourceDbIds) == false)
                {
                    throw new InvalidDataException($"Counter-Group document '{docId}' is missing '{CountersStorage.DbIds}' property. Shouldn't happen");
                }
                
                foreach (LazyStringValue dbId in sourceDbIds)
                {
                    builder.WriteValue(dbId);
                }
                builder.WriteArrayEnd();
                
                
                builder.WritePropertyName(CountersStorage.Values);
                builder.StartWriteObject();
                foreach (var clonedCounterGroupValues in counterGroupsValues)
                {
                    if(clonedCounterGroupValues.TryGet(CountersStorage.Values, out BlittableJsonReaderObject values) == false)
                    {
                        throw new InvalidDataException($"Counter-Group document '{docId}' is missing '{CountersStorage.Values}' property. Shouldn't happen");
                    }
                    
                    foreach (var counterName in values.GetPropertyNames())
                    {
                        builder.WritePropertyName(counterName);
                        BlittableJsonReaderObject.RawBlob rawBlobCounterValue = (BlittableJsonReaderObject.RawBlob)values[counterName];
                        builder.WriteRawBlob(rawBlobCounterValue.Address, rawBlobCounterValue.Length);
                    }
                }
                builder.WriteObjectEnd();
                
                builder.WritePropertyName(CountersStorage.CounterNames);
                builder.StartWriteObject();
                foreach (var clonedCountersGroupValues in counterGroupsValues)
                {
                    if(clonedCountersGroupValues.TryGet(CountersStorage.CounterNames, out BlittableJsonReaderObject counterNamesDict) == false)
                    {
                       throw new InvalidDataException($"Counter-Group document '{docId}' is missing '{CountersStorage.Values}' property. Shouldn't happen");
                    }
                    
                    foreach (var counterNameKey in counterNamesDict.GetPropertyNames())
                    {
                        builder.WritePropertyName(counterNameKey);
                        builder.WriteValue((LazyStringValue)counterNamesDict[counterNameKey]);
                    }
                }
                
                builder.WriteObjectEnd();
                
                builder.WriteObjectEnd();
                builder.FinalizeDocument();

                mergedCounterGroupValues = builder.CreateReader();
            }

            return mergedCounterGroupValues;
        }

        
        private BlittableJsonReaderObject CollectArchivedDocumentCountersIfNeeded(DocumentsOperationContext context, Document doc, string documentId, string collectionName, string archivedCollectionName)
        {
            if ((doc.Flags & DocumentFlags.HasCounters) == 0) return null;
            
            var counterGroups = _database.DocumentsStorage.CountersStorage.GetCounterValuesForDocument(context, documentId).ToList();
                                            
            var clonedCountersGroupsValues = new List<BlittableJsonReaderObject>(counterGroups.Count);
            foreach (var counterGroup in counterGroups)
                clonedCountersGroupsValues.Add(counterGroup.Values.Clone(context));
                                            
            var mergedCounterGroupsValues = BuildMergedCounterGroupValues(context, clonedCountersGroupsValues, documentId);
            
            return mergedCounterGroupsValues;
        }

        private void PutArchivedCounters(DocumentsOperationContext context, string documentId, string archivedCollectionName, BlittableJsonReaderObject mergedCounterGroupsValues)
        {
            var newCounterEtag = _database.DocumentsStorage.GenerateNextEtag();
            var newCounterChangeVector = _database.DocumentsStorage.GetNewChangeVector(context, newCounterEtag);
            _database.DocumentsStorage.CountersStorage.PutCounters(context, documentId, archivedCollectionName, newCounterChangeVector , mergedCounterGroupsValues, newCounterEtag);
        }

        private void ArchiveDocumentTimeSeriesIfNeeded(DocumentsOperationContext context, Document doc, string documentId, string collectionName, string archivedCollectionName)
        {
            
        }
        
        private void ArchiveDocumentRevisionsIfNeeded(DocumentsOperationContext context, Document doc, string documentId, string collectionName, string archivedCollectionName)
        {
            
        }
        
        private void ArchiveDocumentAttachmentsIfNeeded(DocumentsOperationContext context, Document doc, string documentId, string collectionName, string archivedCollectionName)
        {
                    
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
                if (HasPassed(metadata, currentTime) == false) return false;
                        
                var collectionName = metadata[Constants.Documents.Metadata.Collection].ToString();
                string archivedCollectionName = $"{collectionName}Archived";

                // Modify
                metadata.Modifications = new DynamicJsonValue(metadata);
                metadata.Modifications[Constants.Documents.Metadata.Collection] = archivedCollectionName;
                metadata.Modifications[Constants.Documents.Metadata.Archived] = true;
                metadata.Modifications.Remove(Constants.Documents.Metadata.Archive);

                var mergedCounterGroups = CollectArchivedDocumentCountersIfNeeded(context, doc, id, collectionName, archivedCollectionName);
                _database.DocumentsStorage.CountersStorage.DeleteCountersForDocument(context, id, new CollectionName(collectionName));
                
                ArchiveDocumentTimeSeriesIfNeeded(context, doc, id, collectionName, archivedCollectionName);
                ArchiveDocumentAttachmentsIfNeeded(context, doc, id, collectionName, archivedCollectionName);
                ArchiveDocumentRevisionsIfNeeded(context, doc, id, collectionName, archivedCollectionName);

                // Save changes (Re-read) - ReadObject returns modified blittable document basing on the previous one
                // We need to create a new blittable before deleting the previous document, delete frees the memory assigned to it
                // ReadObject would try to fetch document from the memory that could have been already reused
                using (var updated = context.ReadObject(doc.Data, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    _database.DocumentsStorage.Delete(context, lowerId, id, expectedChangeVector: null, deleteCounters: false);
                    if (mergedCounterGroups != null)
                        PutArchivedCounters(context, id, archivedCollectionName, mergedCounterGroups);
                    _database.DocumentsStorage.Put(context, id, doc.ChangeVector, updated, flags: doc.Flags.Strip(DocumentFlags.FromClusterTransaction));
                }
            }

            return true;
        }
        
        public int ArchiveDocuments(DocumentsOperationContext context, Dictionary<Slice, List<(Slice LowerId, string Id)>> toArchive, DateTime currentTime)
        {
            int archivedCount = 0;
            var archiveTree = context.Transaction.InnerTransaction.ReadTree(DocumentsByArchiveDateTime);

            foreach (var pair in toArchive)
            {
                foreach (var ids in pair.Value)
                {
                    bool timePassed = ArchiveDocument(context, ids.LowerId, ids.Id, currentTime);
                    if (timePassed == false) continue;
                    archiveTree.MultiDelete(pair.Key, ids.LowerId);
                    archivedCount++;
                }
            }
            return archivedCount;
        }
    }
}
