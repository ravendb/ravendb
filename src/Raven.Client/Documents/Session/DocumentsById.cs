using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    internal class DocumentsById : IEnumerable<KeyValuePair<string, DocumentInfo>>
    {
        private readonly Dictionary<string, DocumentInfo> _inner;

        public DocumentsById()
        {
            _inner = new Dictionary<string, DocumentInfo>(StringComparer.OrdinalIgnoreCase);
        }

        public bool TryGetValue(string id, out DocumentInfo info)
        {
            return _inner.TryGetValue(id, out info);
        }

        public void Add(DocumentInfo info)
        {
            if (_inner.ContainsKey(info.Id))
                return;

            _inner[info.Id] = info;
        }

        public bool Remove(string id)
        {
            return _inner.Remove(id);
        }

        public void Clear()
        {
            _inner.Clear();
        }

        public int Count => _inner.Count;

        public IEnumerator<KeyValuePair<string, DocumentInfo>> GetEnumerator()
        {
            return _inner.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
