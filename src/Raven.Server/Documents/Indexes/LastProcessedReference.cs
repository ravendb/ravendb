using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes
{
    public class LastProcessedReference
    {
        private readonly Dictionary<string, string> _lastIdPerCollectionForDocuments = new Dictionary<string, string>();

        private readonly Dictionary<string, string> _lastIdPerCollectionForTombstones = new Dictionary<string, string>();

        public void Set(ActionType actionType, string collection, string id)
        {
            var dictionary = GetDictionary(actionType);
            dictionary[collection] = id;
        }

        public string GetDocumentId(ActionType actionType, string collection)
        {
            var dictionary = GetDictionary(actionType);
            return dictionary.TryGetValue(collection, out var id) ? id : null;
        }

        public void Clear(ActionType actionType)
        {
            var dictionary = GetDictionary(actionType);
            dictionary.Clear();
        }

        private Dictionary<string, string> GetDictionary(ActionType actionType)
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
