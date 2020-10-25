using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Workers;

namespace Raven.Server.Documents.Indexes
{
    public class LastProcessedReferences
    {
        private class ReferenceState
        {
            public string ReferencedItemId;
            public string NextItemId;
            public long ReferencedItemEtag;
        }
        
        private readonly Dictionary<string, ReferenceState> _lastIdPerCollectionForDocuments = new Dictionary<string, ReferenceState>();

        private readonly Dictionary<string, ReferenceState> _lastIdPerCollectionForTombstones = new Dictionary<string, ReferenceState>();

        public void Set(HandleReferencesBase.ActionType actionType, string collection, HandleReferencesBase.Reference reference, string itemId)
        {
            GetDictionary(actionType)[collection] = new ReferenceState
            {
                ReferencedItemId = reference.Key, 
                ReferencedItemEtag = reference.Etag, 
                NextItemId = itemId,
            };
        }

        public string GetLastProcessedItemId(HandleReferencesBase.ActionType actionType, string collection, HandleReferencesBase.Reference reference)
        {
            var dictionary = GetDictionary(actionType);
            if (dictionary.TryGetValue(collection, out var state) == false)
                return null;

            if (reference.Key == state.ReferencedItemId && reference.Etag == state.ReferencedItemEtag) 
                return state.NextItemId;
            
            // the document has changed since, cannot continue from the same point
            dictionary.Remove(collection);
            return null;

        }

        public void ClearForCollection(HandleReferencesBase.ActionType actionType, string collection)
        {
            var dictionary = GetDictionary(actionType);
            dictionary.Remove(collection);
        }

        public void Clear()
        {
            _lastIdPerCollectionForDocuments.Clear();
            _lastIdPerCollectionForTombstones.Clear();
        }

        private Dictionary<string, ReferenceState> GetDictionary(HandleReferencesBase.ActionType actionType)
        {
            return actionType == HandleReferencesBase.ActionType.Document
                ? _lastIdPerCollectionForDocuments
                : _lastIdPerCollectionForTombstones;
        }
    }

}
