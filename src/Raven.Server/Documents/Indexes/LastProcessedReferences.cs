using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Indexes.Workers;

namespace Raven.Server.Documents.Indexes
{
    public class LastProcessedReferences
    {
        private readonly Dictionary<HandleReferencesBase.ReferenceType, ReferencesState> _lastProcessedReferencesByType = 
            new Dictionary<HandleReferencesBase.ReferenceType, ReferencesState>();

        public LastProcessedReferences()
        {
            foreach (var type in Enum.GetValues(typeof(HandleReferencesBase.ReferenceType)).Cast<HandleReferencesBase.ReferenceType>())
            {
                _lastProcessedReferencesByType[type] = new ReferencesState();
            }
        }

        public ReferencesState.Reference For(HandleReferencesBase.ReferenceType referenceType, HandleReferencesBase.ActionType actionType, string collection)
        {
            var referencesState = _lastProcessedReferencesByType[referenceType];
            return referencesState.For(actionType, collection);
        }

        public void ClearForType(HandleReferencesBase.ReferenceType type, HandleReferencesBase.ActionType actionType)
        {
            _lastProcessedReferencesByType[type].Clear(actionType);
        }
    }

    public class ReferencesState
    {
        private readonly Dictionary<string, ReferenceState> _lastIdPerCollectionForDocuments = new Dictionary<string, ReferenceState>();

        private readonly Dictionary<string, ReferenceState> _lastIdPerCollectionForTombstones = new Dictionary<string, ReferenceState>();

        public Reference For(HandleReferencesBase.ActionType actionType, string collection)
        {
            return new Reference(GetDictionary(actionType), collection);
        }

        public void Clear(HandleReferencesBase.ActionType actionType)
        {
            var dictionary = GetDictionary(actionType);
            dictionary.Clear();
        }

        private Dictionary<string, ReferenceState> GetDictionary(HandleReferencesBase.ActionType actionType)
        {
            return actionType == HandleReferencesBase.ActionType.Document
                ? _lastIdPerCollectionForDocuments
                : _lastIdPerCollectionForTombstones;
        }

        public class ReferenceState
        {
            public string ReferencedItemId;
            public string NextItemId;
            public long ReferencedItemEtag;
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

            public void Set(HandleReferencesBase.Reference reference, string itemId)
            {
                _dictionary[_collection] = new ReferenceState
                {
                    ReferencedItemId = reference.Key,
                    ReferencedItemEtag = reference.Etag,
                    NextItemId = itemId
                };
            }

            public string GetLastProcessedItemId(HandleReferencesBase.Reference reference)
            {
                if (_dictionary.TryGetValue(_collection, out var state) == false)
                    return null;

                var lastProcessdItemId = reference.Key == state.ReferencedItemId && reference.Etag == state.ReferencedItemEtag
                    ? state.NextItemId
                    : null;

                // - we resume from the same point we stopped before
                // - the document has changed since, cannot continue from the same point
                // either way we need to remove it
                _dictionary.Remove(_collection);
                return lastProcessdItemId;
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
