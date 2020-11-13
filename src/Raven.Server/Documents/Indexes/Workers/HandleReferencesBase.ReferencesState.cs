using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers
{
    public abstract partial class HandleReferencesBase
    {
        private class ReferencesState
        {
            private readonly Dictionary<string, ReferenceState> _lastIdPerCollectionForDocuments = new Dictionary<string, ReferenceState>();

            private readonly Dictionary<string, ReferenceState> _lastIdPerCollectionForTombstones = new Dictionary<string, ReferenceState>();

            public ReferenceState For(ActionType actionType, string collection)
            {
                var dictionary = GetDictionary(actionType);
                return dictionary.TryGetValue(collection, out var referenceState) ? referenceState : null;
            }

            public InMemoryReferencesInfo GetReferencesInfo(string collection)
            {
                return new InMemoryReferencesInfo
                {
                    ParentItemEtag = For(ActionType.Document, collection)?.GetLastIndexedParentEtag() ?? 0,
                    ParentTombstoneEtag = For(ActionType.Tombstone, collection)?.GetLastIndexedParentEtag() ?? 0,
                };
            }

            public void Clear(ActionType actionType)
            {
                var dictionary = GetDictionary(actionType);
                dictionary.Clear();
            }

            public void Set(ActionType actionType, string collection, Reference reference, string itemId, long lastIndexedParentEtag, TransactionOperationContext indexContext)
            {
                var dictionary = GetDictionary(actionType);
                var referencedItemId = (string)reference.Key;

                indexContext.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += _ =>
                {
                    // we update this only after the transaction was committed
                    dictionary[collection] = new ReferenceState(referencedItemId, reference.Etag, itemId, lastIndexedParentEtag);
                };
            }

            public void Clear(bool earlyExit, ActionType actionType, string collection, TransactionOperationContext indexContext)
            {
                if (earlyExit)
                    return;

                var dictionary = GetDictionary(actionType);
                if (dictionary.Count == 0)
                    return;

                indexContext.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += _ =>
                {
                    // we update this only after the transaction was committed
                    dictionary.Remove(collection);
                };
            }

            private Dictionary<string, ReferenceState> GetDictionary(ActionType actionType)
            {
                return actionType == ActionType.Document
                    ? _lastIdPerCollectionForDocuments
                    : _lastIdPerCollectionForTombstones;
            }

            public class ReferenceState
            {
                private readonly string _referencedItemId;
                private readonly string _nextItemId;
                private readonly long _referencedItemEtag;
                private readonly long _lastIndexedParentEtag;

                public ReferenceState(string referencedItemId, long referenceEtag, string itemId, long lastIndexedParentEtag)
                {
                    _referencedItemId = referencedItemId;
                    _referencedItemEtag = referenceEtag;
                    _nextItemId = itemId;
                    _lastIndexedParentEtag = lastIndexedParentEtag;
                }

                public string GetLastProcessedItemId(Reference referencedDocument)
                {
                    if (referencedDocument.Key == _referencedItemId && referencedDocument.Etag == _referencedItemEtag)
                        return _nextItemId;

                    return null;
                }

                public long GetLastIndexedParentEtag()
                {
                    return _lastIndexedParentEtag;
                }
            }
        }
    }
}
