using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Workers;

namespace Raven.Server.Documents.Indexes
{
    public class LastProcessedReference
    {
        public enum ActionType
        {
            Document,
            Tombstone
        }
        private class ReferenceState
        {
            public string ReferencedDocumentId;
            public string NextItemId;
            public long ReferenceDocumentEtag;
        }
        
        private readonly Dictionary<string, ReferenceState> _lastIdPerCollectionForDocuments = new Dictionary<string, ReferenceState>();

        private readonly Dictionary<string, ReferenceState> _lastIdPerCollectionForTombstones = new Dictionary<string, ReferenceState>();

        public void Set(ActionType actionType, string collection, HandleReferencesBase.Reference referencedDocument, string itemId)
        {
            GetDictionary(actionType)[collection] = new ReferenceState
            {
                ReferencedDocumentId = referencedDocument.Key, 
                ReferenceDocumentEtag = referencedDocument.Etag, 
                NextItemId = itemId,
            };
        }

        public string GetLastProcessedItemId(ActionType actionType, string collection, HandleReferencesBase.Reference referencedDocument)
        {
            var dictionary = GetDictionary(actionType);
            if (dictionary.TryGetValue(collection, out var state) == false)
                return null;

            if (referencedDocument.Key == state.ReferencedDocumentId && referencedDocument.Etag == state.ReferenceDocumentEtag) 
                return state.NextItemId;
            
            // the document has changed since, cannot continue from the same point
            dictionary.Remove(collection);
            return null;

        }

        public void Clear(ActionType actionType)
        {
            GetDictionary(actionType).Clear();
        }

        private Dictionary<string, ReferenceState> GetDictionary(ActionType actionType)
        {
            return actionType == ActionType.Document
                ? _lastIdPerCollectionForDocuments
                : _lastIdPerCollectionForTombstones;
        }
    }

}
