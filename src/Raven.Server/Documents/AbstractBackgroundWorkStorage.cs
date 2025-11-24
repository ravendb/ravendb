using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.BackgroundWork;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents;

public abstract unsafe class AbstractBackgroundWorkStorage<TWorkInfo> : AbstractBackgroundWorkStorageBase
    where TWorkInfo : DocumentExpirationInfo, new()
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

    protected abstract void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string identifier, DateTime currentTime);
    protected abstract void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<TWorkInfo> expiredDocs, ref int totalCount);
    protected abstract TWorkInfo GetBackgroundWorkInfo(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice);

    [DoesNotReturn]
    protected void ThrowWrongDateFormat(Slice treeKey, string expirationDate)
    {
        throw new InvalidOperationException(
            $"The due date format for document '{treeKey}' is not valid: '{expirationDate}'. Use the following format: {Database.Time.GetUtcNow():O}");
    }

    public void Put(DocumentsOperationContext context, Slice lowerId, string processDateString)
    {
        if (DateTime.TryParseExact(processDateString, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind,
                out DateTime processDate) == false)
            ThrowWrongDateFormat(lowerId, processDateString);

        // We explicitly enable adding items that have already been expired, we have to, because if the time lag is short, it is possible
        // that we add an item that expire in 1 second, but by the time we process it, it already expired. The user did nothing wrong here
        // and we'll use the normal cleanup routine to clean things up later.

        var processDateUniversalTime = processDate.ToUniversalTime();
        var ticksBigEndian = Bits.SwapBytes(processDateUniversalTime.Ticks);

        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiAdd(ticksSlice, lowerId);
    }

    public Queue<TWorkInfo> GetDocuments(BackgroundWorkParameters options, ref int totalCount, out Stopwatch duration, CancellationToken cancellationToken)
    {
        var currentTicks = options.CurrentTime.Ticks;
        var isFirstInTopology = ShouldHandleWorkOnCurrentNode(options.DatabaseTopology, options.NodeTag);
        var entriesTree = options.Context.Transaction.InnerTransaction.ReadTree(_treeName);
        using (var it = entriesTree.Iterate(false))
        {
            if (it.Seek(Slices.BeforeAllKeys) == false)
            {
                duration = null;
                return null;
            }

            var toProcess = new Queue<TWorkInfo>();
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
                            TWorkInfo item;
                            try
                            {
                                item = GetBackgroundWorkInfo(options, clonedId, ticksAsSlice);
                            }
                            catch (DocumentConflictException)
                            {
                                item = new TWorkInfo { Status = BackgroundWorkInfoStatus.Conflict };
                            }

                            switch (item.Status)
                            {
                                case BackgroundWorkInfoStatus.Process when isFirstInTopology == false:
                                case BackgroundWorkInfoStatus.Conflict when isFirstInTopology == false:
                                    break;
                                case BackgroundWorkInfoStatus.Process:
                                case BackgroundWorkInfoStatus.Delete:
                                    toProcess.Enqueue(item);
                                    totalCount++;
                                    break;
                                case BackgroundWorkInfoStatus.Conflict:
                                    HandleDocumentConflict(options, ticksAsSlice, clonedId, toProcess, ref totalCount);
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException();
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

    public int ProcessDocuments(DocumentsOperationContext context, Queue<TWorkInfo> toProcess, DateTime currentTime)
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
