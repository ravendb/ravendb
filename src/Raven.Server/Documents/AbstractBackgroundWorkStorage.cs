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


namespace Raven.Server.Documents;

public abstract unsafe class AbstractBackgroundWorkStorage
{
    protected readonly DocumentDatabase Database;
    protected readonly DocumentsStorage DocumentsStorage;
    protected readonly Logger Logger;

    protected AbstractBackgroundWorkStorage(DocumentDatabase database, Logger logger)
    {
        Logger = logger;
        Database = database;
        DocumentsStorage = Database.DocumentsStorage;
        
    }

    protected void PutInternal(DocumentsOperationContext context, Slice lowerId, string expirationDate, string treeName)
    {
        if (DateTime.TryParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind,
                out DateTime date) == false)
            ThrowWrongDateFormat(lowerId, expirationDate);

        // We explicitly enable adding documents that have already been expired, we have to, because if the time lag is short, it is possible
        // that we add a document that expire in 1 second, but by the time we process it, it already expired. The user did nothing wrong here
        // and we'll use the normal cleanup routine to clean things up later.

        var expiry = date.ToUniversalTime();
        var ticksBigEndian = Bits.SwapBytes(expiry.Ticks);

        var tree = context.Transaction.InnerTransaction.ReadTree(treeName);
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiAdd(ticksSlice, lowerId);
    }
    
    [DoesNotReturn]
    private void ThrowWrongDateFormat(Slice lowerId, string expirationDate)
    {
        throw new InvalidOperationException(
            $"The due date format for document '{lowerId}' is not valid: '{expirationDate}'. Use the following format: {Database.Time.GetUtcNow():O}");
    }
    
    
    public abstract record BackgroundProcessDocumentsOptions
    {
        public DocumentsOperationContext Context;
        public DateTime CurrentTime;
        public DatabaseTopology DatabaseTopology;
        public string NodeTag;
        public long AmountToTake;

        protected BackgroundProcessDocumentsOptions(DocumentsOperationContext context, DateTime currentTime, DatabaseTopology topology, string nodeTag, long amountToTake) =>
            (Context, CurrentTime, DatabaseTopology, NodeTag, AmountToTake)
            = (context, currentTime, topology, nodeTag, amountToTake);
    }

    protected Dictionary<Slice, List<(Slice LowerId, string Id)>> GetDocuments(BackgroundProcessDocumentsOptions options, string treeName, string metadataPropertyToCheck,
        out Stopwatch duration, CancellationToken cancellationToken)
    {
        var count = 0;
        var currentTicks = options.CurrentTime.Ticks;

        var expirationTree = options.Context.Transaction.InnerTransaction.ReadTree(treeName);
        using (var it = expirationTree.Iterate(false))
        {
            if (it.Seek(Slices.BeforeAllKeys) == false)
            {
                duration = null;
                return null;
            }

            var expired = new Dictionary<Slice, List<(Slice LowerId, string Id)>>();
            duration = Stopwatch.StartNew();

            do
            {
                var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                if (entryTicks > currentTicks)
                    break;

                var ticksAsSlice = it.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                var expiredDocs = new List<(Slice LowerId, string Id)>();

                using (var multiIt = expirationTree.MultiRead(it.CurrentKey))
                {
                    if (multiIt.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            if (cancellationToken.IsCancellationRequested)
                                return expired;

                            var clonedId = multiIt.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                            try
                            {
                                using (var document = Database.DocumentsStorage.Get(options.Context, clonedId,
                                           DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector))
                                {
                                    if (document == null ||
                                        document.TryGetMetadata(out var metadata) == false ||
                                        HasPassed(metadata, options.CurrentTime, metadataPropertyToCheck) ==
                                        false)
                                    {
                                        expiredDocs.Add((clonedId, null));
                                        continue;
                                    }

                                    if (ShouldHandleWorkOnCurrentNode(options.DatabaseTopology, options.NodeTag) == false)
                                        break;

                                    expiredDocs.Add((clonedId, document.Id));
                                }
                            }
                            catch (DocumentConflictException)
                            {
                                HandleDocumentConflict(options, clonedId, ref expiredDocs);
                            }
                        } while (multiIt.MoveNext() && expiredDocs.Count + count < options.AmountToTake);
                    }
                }

                count += expiredDocs.Count;
                if (expiredDocs.Count > 0)
                    expired.Add(ticksAsSlice, expiredDocs);

            } while (it.MoveNext() && count < options.AmountToTake);

            return expired;
        }
    }

    protected abstract void HandleDocumentConflict(BackgroundProcessDocumentsOptions options, Slice clonedId, ref List<(Slice LowerId, string Id)> docsToProcess);

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
}

