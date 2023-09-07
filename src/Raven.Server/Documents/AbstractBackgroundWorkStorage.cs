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
using Sparrow.Logging;
using Voron;
using Voron.Impl;


namespace Raven.Server.Documents;

public abstract unsafe class AbstractBackgroundWorkStorage
{
    protected readonly DocumentDatabase Database;
    protected readonly DocumentsStorage DocumentsStorage;
    protected readonly Logger Logger;
    protected readonly string MetadataPropertyName;
    private readonly string _treeName;

    protected AbstractBackgroundWorkStorage(Transaction tx, DocumentDatabase database, Logger logger, string treeName, string metadataPropertyName)
    {
        tx.CreateTree(treeName);
        
        Logger = logger;
        Database = database;
        DocumentsStorage = Database.DocumentsStorage;
        _treeName = treeName;
        MetadataPropertyName = metadataPropertyName;
    }

    public void Put(DocumentsOperationContext context, Slice lowerId, string processDateString)
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
    
    


    public Dictionary<Slice, List<(Slice LowerId, string Id)>> GetDocuments(BackgroundWorkParameters options, out Stopwatch duration, CancellationToken cancellationToken)
    {
        var count = 0;
        var currentTicks = options.CurrentTime.Ticks;

        var entriesTree = options.Context.Transaction.InnerTransaction.ReadTree(_treeName);
        using (var it = entriesTree.Iterate(false))
        {
            if (it.Seek(Slices.BeforeAllKeys) == false)
            {
                duration = null;
                return null;
            }

            var toProcess = new Dictionary<Slice, List<(Slice LowerId, string Id)>>();
            duration = Stopwatch.StartNew();

            do
            {
                var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                if (entryTicks > currentTicks)
                    break;

                var ticksAsSlice = it.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                var docsToProcess = new List<(Slice LowerId, string Id)>();

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
                                using (var document = Database.DocumentsStorage.Get(options.Context, clonedId,
                                           DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector))
                                {
                                    if (document == null ||
                                        document.TryGetMetadata(out var metadata) == false ||
                                        HasPassed(metadata, options.CurrentTime, MetadataPropertyName) ==
                                        false)
                                    {
                                        docsToProcess.Add((clonedId, null));
                                        continue;
                                    }

                                    if (ShouldHandleWorkOnCurrentNode(options.DatabaseTopology, options.NodeTag) == false)
                                        break;

                                    docsToProcess.Add((clonedId, document.Id));
                                }
                            }
                            catch (DocumentConflictException)
                            {
                                HandleDocumentConflict(options, clonedId, ref docsToProcess);
                            }
                        } while (multiIt.MoveNext() && docsToProcess.Count + count < options.AmountToTake);
                    }
                }

                count += docsToProcess.Count;
                if (docsToProcess.Count > 0)
                    toProcess.Add(ticksAsSlice, docsToProcess);

            } while (it.MoveNext() && count < options.AmountToTake);

            return toProcess;
        }
    }

    protected abstract void HandleDocumentConflict(BackgroundWorkParameters options, Slice clonedId, ref List<(Slice LowerId, string Id)> docsToProcess);

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

    public int ProcessDocuments(DocumentsOperationContext context, Dictionary<Slice, List<(Slice LowerId, string Id)>> docsToProcess, DateTime currentTime)
    {
        var processedCount = 0;
        var docsTree = context.Transaction.InnerTransaction.ReadTree(_treeName);

        foreach (var pair in docsToProcess)
        {
            foreach (var ids in pair.Value)
            {
                if (ids.Id != null)
                {
                    ProcessDocument(context, ids.LowerId, ids.Id, currentTime);
                    processedCount++;
                }

                docsTree.MultiDelete(pair.Key, ids.LowerId);
            }
        }

        return processedCount;
    }
    
}

