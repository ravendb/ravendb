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

            public Reference For(ActionType actionType, string collection)
            {
                return new Reference(GetDictionary(actionType), collection);
            }

            public InMemoryReferencesInfo GetReferencesInfo(string collection)
            {
                return new InMemoryReferencesInfo
                {
                    ReferencedItemEtag = new Reference(GetDictionary(ActionType.Document), collection).GetLastIndexedParentEtag(),
                    ReferencedTombstoneEtag = new Reference(GetDictionary(ActionType.Tombstone), collection).GetLastIndexedParentEtag(),
                };
            }

            public void Clear(ActionType actionType)
            {
                var dictionary = GetDictionary(actionType);
                dictionary.Clear();
            }

            private Dictionary<string, ReferenceState> GetDictionary(ActionType actionType)
            {
                return actionType == ActionType.Document
                    ? _lastIdPerCollectionForDocuments
                    : _lastIdPerCollectionForTombstones;
            }

            public class ReferenceState
            {
                public string ReferencedItemId;
                public string NextItemId;
                public long ReferencedItemEtag;
                public long LastIndexedParentEtag;
            }

            public class Reference
            {
                private readonly Dictionary<string, ReferenceState> _dictionary;
                private readonly string _collection;

                public Reference(Dictionary<string, ReferenceState> dictionary, string collection)
                {
                    _dictionary = dictionary;
                    _collection = collection;
                }

                public void Set(HandleReferencesBase.Reference reference, string itemId, long lastIndexedParentEtag, TransactionOperationContext indexContext)
                {
                    var referencedItemId = (string)reference.Key;

                    indexContext.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += _ =>
                    {
                        // we update this only after the transaction was commited
                        _dictionary[_collection] = new ReferenceState
                        {
                            ReferencedItemId = referencedItemId,
                            ReferencedItemEtag = reference.Etag,
                            NextItemId = itemId,
                            LastIndexedParentEtag = lastIndexedParentEtag
                        };
                    };
                }

                public string GetLastProcessedItemId(HandleReferencesBase.Reference reference)
                {
                    if (_dictionary.TryGetValue(_collection, out var state) == false)
                        return null;

                    if (reference.Key == state.ReferencedItemId && reference.Etag == state.ReferencedItemEtag)
                        return state.NextItemId;

                    return null;
                }

                public long GetLastIndexedParentEtag()
                {
                    if (_dictionary.TryGetValue(_collection, out var state) == false)
                        return 0;

                    return state.LastIndexedParentEtag;
                }

                public void Clear(bool earlyExit)
                {
                    if (earlyExit)
                        return;

                    _dictionary.Remove(_collection);
                }
            }
        }
    }
}
