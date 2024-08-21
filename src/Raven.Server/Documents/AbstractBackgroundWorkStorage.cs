using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading;
using Raven.Client.Exceptions.Documents;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Documents;

public abstract unsafe class AbstractBackgroundWorkStorage
{
    protected readonly DocumentDatabase Database;
    protected readonly string MetadataPropertyName;
    protected readonly string _treeName;

    protected AbstractBackgroundWorkStorage(Transaction tx, DocumentDatabase database, string treeName, string metadataPropertyName)
    {
        tx.CreateTree(treeName);

        Database = database;
        _treeName = treeName;
        MetadataPropertyName = metadataPropertyName;
    }

    public virtual void Put(DocumentsOperationContext context, Slice lowerId, string processDateString)
    {
        if (DateTime.TryParseExact(processDateString, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind,
                out DateTime processDate) == false)
            ThrowWrongDateFormat(lowerId, processDateString);

        // We explicitly enable adding documents that have already been expired, we have to, because if the time lag is short, it is possible
        // that we add a document that expire in 1 second, but by the time we process it, it already expired. The user did nothing wrong here
        // and we'll use the normal cleanup routine to clean things up later.

        var processDateUniversalTime = processDate.ToUniversalTime();
        var ticksBigEndian = Bits.SwapBytes(processDateUniversalTime.Ticks);

        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiAdd(ticksSlice, lowerId);
    }
    
    [DoesNotReturn]
    private void ThrowWrongDateFormat(Slice lowerId, string expirationDate)
    {
        throw new InvalidOperationException(
            $"The due date format for document '{lowerId}' is not valid: '{expirationDate}'. Use the following format: {Database.Time.GetUtcNow():O}");
    }

    public Queue<DocumentExpirationInfo> GetDocuments(BackgroundWorkParameters options, ref int totalCount, out Stopwatch duration, CancellationToken cancellationToken)
    {
        var currentTicks = options.CurrentTime.Ticks;

        var entriesTree = options.Context.Transaction.InnerTransaction.ReadTree(_treeName);
        using (var it = entriesTree.Iterate(false))
        {
            if (it.Seek(Slices.BeforeAllKeys) == false)
            {
                duration = null;
                return null;
            }

            var toProcess = new Queue<DocumentExpirationInfo>();
            duration = Stopwatch.StartNew();

            do
            {
                var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                if (entryTicks > currentTicks)
                    break;

                var ticksAsSlice = it.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                using (var multiIt = entriesTree.MultiRead(it.CurrentKey))
                {
                    if (multiIt.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            if (cancellationToken.IsCancellationRequested)
                                return toProcess;

                            var clonedId = multiIt.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                            try
                            {
                                var item = GetDocumentAndIdOrCollection(options, clonedId, ticksAsSlice);
                                if (item.Document == null)
                                {
                                    toProcess.Enqueue(item);
                                    totalCount++;
                                    continue;
                                }

                                using (item.GetDocumentDisposable())
                                {
                                    if (ShouldHandleWorkOnCurrentNode(options.DatabaseRecord.Topology, options.NodeTag) == false)
                                        break;

                                    toProcess.Enqueue(item);
                                    totalCount++;
                                    options.Context.Transaction.ForgetAbout(item.Document);
                                }
                            }
                            catch (DocumentConflictException)
                            {
                                HandleDocumentConflict(options, ticksAsSlice, clonedId, toProcess, ref totalCount);
                            }

                        } while (multiIt.MoveNext()
                                 && toProcess.Count < options.AmountToTake 
                                 && totalCount < options.MaxItemsToProcess);
                    }
                }
            } while (it.MoveNext() 
                     && toProcess.Count < options.AmountToTake
                     && totalCount < options.MaxItemsToProcess);

            return toProcess;
        }
    }

    public class DocumentExpirationInfo
    {
        public Document Document { get; set; }
        public Slice Ticks { get; }
        public Slice LowerId { get; }
        public string Id { get; }

        private DocumentExpirationInfo()
        {
        }

        public DocumentExpirationInfo(Slice ticks, Slice lowerId, string id)
        {
            Ticks = ticks;
            LowerId = lowerId;
            Id = id;
        }

        public IDisposable GetDocumentDisposable()
        {
            return new DisposableAction(() =>
            {
                Document?.Dispose();
                Document = null;
            });
        }
    }

    protected virtual DocumentExpirationInfo GetDocumentAndIdOrCollection(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        var document = Database.DocumentsStorage.Get(options.Context, clonedId, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector);
        if (document == null ||
            document.TryGetMetadata(out var metadata) == false ||
            HasPassed(metadata, options.CurrentTime, MetadataPropertyName) == false)
        {
            return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
        }

        return new DocumentExpirationInfo(ticksSlice, clonedId, id: document.Id)
        {
            Document = document
        };
    }

    protected abstract void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<DocumentExpirationInfo> expiredDocs, ref int totalCount);

    protected static bool ShouldHandleWorkOnCurrentNode(DatabaseTopology topology, string nodeTag)
    {
        var isFirstInTopology = string.Equals(topology.AllNodes.FirstOrDefault(), nodeTag, StringComparison.OrdinalIgnoreCase);
        if (isFirstInTopology == false)
        {
            // this can happen when we are running the expiration/refresh/data archival on a node that isn't 
            // the primary node for the database. In this case, we still run the cleanup
            // procedure, but we only account for documents that have already been 
            // marked for processing, to cleanup the queue. We'll stop on the first
            // document that is scheduled to be processed (expired/refreshed/archived) and wait until the 
            // primary node will act on it. In this way, we reduce conflicts between nodes
            // performing the same action concurrently.     
            return false;
        }

        return true;
    }

    public static bool HasPassed(BlittableJsonReaderObject metadata, DateTime currentTime, string metadataPropertyToCheck)
    {
        if (metadata.TryGet(metadataPropertyToCheck, out LazyStringValue dateFromMetadata) == false) 
            return false;
        
        if (LazyStringParser.TryParseDateTime(dateFromMetadata.Buffer, dateFromMetadata.Length, out DateTime date, out _, properlyParseThreeDigitsMilliseconds: true) != LazyStringParser.Result.DateTime)
            if (DateTime.TryParseExact(dateFromMetadata.ToString(CultureInfo.InvariantCulture), DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind, out date) == false)
                return false;
        
        if (date.Kind != DateTimeKind.Utc) 
            date = date.ToUniversalTime();
                
        return currentTime >= date;
    }
    
    protected abstract void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime);

    public int ProcessDocuments(DocumentsOperationContext context, Queue<DocumentExpirationInfo> toProcess, DateTime currentTime)
    {
        var processedCount = 0;
        var dequeueCount = 0;

        var docsTree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        foreach (var info in toProcess)
        {
            if (info.Id != null)
            {
                ProcessDocument(context, info.LowerId, info.Id, currentTime);
                processedCount++;
            }

            dequeueCount++;
            docsTree.MultiDelete(info.Ticks, info.LowerId);

            if (context.CanContinueTransaction == false)
                break;
        }

        var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
        tx.OnDispose += _ =>
        {
            if (tx.Committed == false)
                return;

            for (int i = 0; i < dequeueCount; i++)
            {
                toProcess.Dequeue();
            }
        };

        return processedCount;
    }
    
}

