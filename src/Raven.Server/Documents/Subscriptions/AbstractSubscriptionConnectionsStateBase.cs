using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Subscriptions;

public abstract class AbstractSubscriptionConnectionsStateBase : IDisposable
{
    private AsyncManualResetEvent _waitForMoreDocuments;
    public readonly CancellationTokenSource CancellationTokenSource;
    protected string _subscriptionName;
    public string SubscriptionName => _subscriptionName;
    public string Query;
    public string LastChangeVectorSent;

    protected AbstractSubscriptionConnectionsStateBase(CancellationToken token)
    {
        CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
        _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
    }

    public abstract bool IsSubscriptionActive();
    public void NotifyHasMoreDocs() => _waitForMoreDocuments.Set();

    public Task<bool> WaitForMoreDocs()
    {
        var t = _waitForMoreDocuments.WaitAsync();
        _waitForMoreDocuments.Reset();
        return t;
    }
    
    public static IEnumerable<ResendItem> GetResendItems(ClusterOperationContext context, string database, long id)
    {
        var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
        using (GetDatabaseAndSubscriptionPrefix(context, database, id, out var prefix))
        using (Slice.External(context.Allocator, prefix, out var prefixSlice))
        {
            foreach (var item in subscriptionState.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
            {
                yield return new ResendItem
                {
                    Type = (SubscriptionType)item.Key[prefixSlice.Size],
                    Id = item.Value.Reader.ReadStringWithPrefix((int)ClusterStateMachine.SubscriptionStateTable.Key, prefix.Length + 2),
                    ChangeVector = item.Value.Reader.ReadString((int)ClusterStateMachine.SubscriptionStateTable.ChangeVector),
                    Batch = Bits.SwapBytes(item.Value.Reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId))
                };
            }
        }
    }

    public static IEnumerable<DocumentRecord> GetDocumentsFromResend(ClusterOperationContext context, string database, long subscriptionId, HashSet<long> activeBatches)
    {
        var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
        using (GetDatabaseAndSubscriptionKeyPrefix(context, database, subscriptionId, SubscriptionType.Document, out var prefix))
        using (Slice.External(context.Allocator, prefix, out var prefixSlice))
        {
            foreach (var (_, tvh) in subscriptionState.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
            {
                long batchId = Bits.SwapBytes(tvh.Reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));
                if (activeBatches.Contains(batchId))
                    continue;

                var id = tvh.Reader.ReadStringWithPrefix((int)ClusterStateMachine.SubscriptionStateTable.Key, prefix.Length);
                var cv = tvh.Reader.ReadString((int)ClusterStateMachine.SubscriptionStateTable.ChangeVector);

                yield return new DocumentRecord { DocumentId = id, ChangeVector = cv };
            }
        }
    }

    public static long GetNumberOfResendDocuments(ServerStore store, string database, SubscriptionType type, long id)
    {
        using (store.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (GetDatabaseAndSubscriptionKeyPrefix(context, database, id, type, out var prefix))
            using (Slice.External(context.Allocator, prefix, out var prefixSlice))
            {
                return subscriptionState.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0).Count();
            }
        }
    }
    
    public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetDatabaseAndSubscriptionKeyPrefix(ClusterOperationContext context, string database, long subscriptionId, SubscriptionType type, out ByteString prefix)
    {
        using var _ = Slice.From(context.Allocator, database.ToLowerInvariant(), out var dbName);
        var rc = context.Allocator.Allocate(dbName.Size + sizeof(byte) + sizeof(long) + sizeof(byte) + sizeof(byte) + sizeof(byte), out prefix);

        PopulatePrefix(subscriptionId, type, ref prefix, ref dbName, out var __);

        return rc;
    }

    public static bool TryGetDocumentFromResend(ClusterOperationContext context, string database, long subscriptionId, string documentId, out string changeVector)
    {
        changeVector = null;
        var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
        using (GetDatabaseAndSubscriptionAndDocumentKey(context, database, subscriptionId, documentId, out var key))
        using (Slice.External(context.Allocator,key, out var keySlice))
        {
            if (subscriptionState.ReadByKey(keySlice, out var reader) == false)
                return false;

            changeVector = reader.ReadString((int)ClusterStateMachine.SubscriptionStateTable.ChangeVector);
            return true;
        }
    }

    public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetDatabaseAndSubscriptionAndDocumentKey(ClusterOperationContext context, string database, long subscriptionId, string documentId, out ByteString key)
    {
        return GetSubscriptionStateKey(context, database, subscriptionId, documentId, SubscriptionType.Document, out key);
    }

    public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetDatabaseAndSubscriptionAndRevisionKey(ClusterOperationContext context, string database, long subscriptionId, string documentId, string currentChangeVector, out ByteString key)
    {
        return GetSubscriptionStateKey(context, database, subscriptionId, currentChangeVector, SubscriptionType.Revision, out key);
    }

    public static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope GetDatabaseAndSubscriptionPrefix(ClusterOperationContext context, string database, long subscriptionId, out ByteString prefix)
    {
        using var _ = Slice.From(context.Allocator, database.ToLowerInvariant(), out var dbName);
        var rc = context.Allocator.Allocate(dbName.Size + sizeof(byte) + sizeof(long) + sizeof(byte), out prefix);

        dbName.CopyTo(prefix.Ptr);
        var position = dbName.Size;

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;
        position++;

        *(long*)(prefix.Ptr + position) = subscriptionId;
        position += sizeof(long);

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;

        return rc;
    }

    public static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope GetSubscriptionStateKey(ClusterOperationContext context, string database, long subscriptionId, string pk, SubscriptionType type, out ByteString key)
    {
        switch (type)
        {
            case SubscriptionType.Document:
                pk = pk.ToLowerInvariant();
                break;
            case SubscriptionType.Revision:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
        }

        using var _ = Slice.From(context.Allocator, database.ToLowerInvariant(), out var dbName);
        using var __ = Slice.From(context.Allocator, pk, out var pkSlice);
        var rc = context.Allocator.Allocate(dbName.Size + sizeof(byte) + sizeof(long) + sizeof(byte) + sizeof(byte) + sizeof(byte) + pkSlice.Size, out key);

        PopulatePrefix(subscriptionId, type, ref key, ref dbName, out int position);

        pkSlice.CopyTo(key.Ptr + position);
        return rc;
    }
        
    private static unsafe void PopulatePrefix(long subscriptionId, SubscriptionType type, ref ByteString prefix, ref Slice dbName, out int position)
    {
        dbName.CopyTo(prefix.Ptr);
        position = dbName.Size;

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;
        position++;

        *(long*)(prefix.Ptr + position) = subscriptionId;
        position += sizeof(long);

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;
        position++;

        *(prefix.Ptr + position) = (byte)type;
        position++;

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;
        position++;
    }

    public abstract void Dispose();
}
