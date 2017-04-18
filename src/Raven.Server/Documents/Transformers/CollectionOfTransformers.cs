using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Raven.Server.Documents.Transformers
{
    public class CollectionOfTransformers : IEnumerable<Transformer>
    {
        private readonly ConcurrentDictionary<long, Transformer> _transformersByEtag = new ConcurrentDictionary<long, Transformer>();
        private readonly ConcurrentDictionary<string, Transformer> _transformersByName = new ConcurrentDictionary<string, Transformer>(StringComparer.OrdinalIgnoreCase);

        public void Add(Transformer transformer)
        {
            _transformersByEtag[transformer.Etag] = transformer;
            _transformersByName[transformer.Name] = transformer;
        }

        public bool TryGetByEtag(long etag, out Transformer index)
        {
            return _transformersByEtag.TryGetValue(etag, out index);
        }

        public bool TryGetByName(string name, out Transformer index)
        {
            return _transformersByName.TryGetValue(name, out index);
        }

        public bool TryRemoveByEtag(long etag, out Transformer index)
        {
            var result = _transformersByEtag.TryRemove(etag, out index);
            if (result == false)
                return false;

            _transformersByName.TryRemove(index.Name, out index);

            return true;
        }

        public IEnumerator<Transformer> GetEnumerator()
        {
            return _transformersByEtag.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => _transformersByEtag.Count;

        public void RenameTransformer(Transformer transformer, string oldName, string newName)
        {
            _transformersByName.AddOrUpdate(newName, transformer, (key, oldValue) => transformer);
            Transformer _;
            _transformersByName.TryRemove(oldName, out _);
        }
    }
}