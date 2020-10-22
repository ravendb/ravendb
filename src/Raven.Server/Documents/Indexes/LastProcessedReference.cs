using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Workers;

namespace Raven.Server.Documents.Indexes
{
    public class LastProcessedReference
    {
        private readonly Dictionary<(string Collection, string ReferencedDocumentId), (long ReferencedDocumentEtag, string ItemId)> _lastIdPerCollectionForDocuments = new Dictionary<(string, string), (long, string)>();

        private readonly Dictionary<(string Collection, string ReferencedDocumentId), (long ReferencedDocumentEtag, string ItemId)> _lastIdPerCollectionForTombstones = new Dictionary<(string, string), (long, string)>();

        public void Set(ActionType actionType, string collection, HandleReferencesBase.Reference referencedDocument, string itemId)
        {
            var dictionary = GetDictionary(actionType);
            dictionary[(collection, referencedDocument.Key)] = (referencedDocument.Etag, itemId);
        }

        public string GetLastProcessedItemId(ActionType actionType, string collection, HandleReferencesBase.Reference referencedDocument)
        {
            var dictionary = GetDictionary(actionType);
            if (dictionary.TryGetValue((collection, referencedDocument.Key), out var tuple) == false)
                return null;

            if (referencedDocument.Etag != tuple.ReferencedDocumentEtag)
            {
                // the document has changed since, cannot continue from the same point
                dictionary.Remove((collection, referencedDocument.Key));
                return null;
            }

            return tuple.ItemId;
        }

        public void Clear(ActionType actionType)
        {
            var dictionary = GetDictionary(actionType);
            dictionary.Clear();
        }

        private Dictionary<(string Collection, string ReferencedDocumentId), (long ReferencedDocumentEtag, string ItemId)> GetDictionary(ActionType actionType)
        {
            var dictionary = actionType == ActionType.Document
                ? _lastIdPerCollectionForDocuments
                : _lastIdPerCollectionForTombstones;

            return dictionary;
        }
    }

    public enum ActionType
    {
        Document,
        Tombstone
    }
}
