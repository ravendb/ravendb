using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Raven.Server.Documents.Transformers
{
    public class CollectionOfTransformers : IEnumerable<Transformer>
    {
        private readonly ConcurrentDictionary<int, Transformer> _transformersById = new ConcurrentDictionary<int, Transformer>();
        private readonly ConcurrentDictionary<string, Transformer> _transformersByName = new ConcurrentDictionary<string, Transformer>(StringComparer.OrdinalIgnoreCase);
        

        public void Add(Transformer transformer)
        {
            _transformersById[transformer.TransformerId] = transformer;
            _transformersByName[transformer.Name] = transformer;
        }

        public bool TryGetById(int id, out Transformer index)
        {
            return _transformersById.TryGetValue(id, out index);
        }

        public bool TryGetByName(string name, out Transformer index)
        {
            return _transformersByName.TryGetValue(name, out index);
        }

        public bool TryRemoveById(int id, out Transformer index)
        {
            var result = _transformersById.TryRemove(id, out index);
            if (result == false)
                return false;

            _transformersByName.TryRemove(index.Name, out index);

            return true;
        }

        public IEnumerator<Transformer> GetEnumerator()
        {
            return _transformersById.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => _transformersById.Count;

        public void RenameTransformer(Transformer transformer, string oldName, string newName)
        {
            _transformersByName.AddOrUpdate(newName, transformer, (key, oldValue) => transformer);
            Transformer _;
            _transformersByName.TryRemove(oldName, out _);
        }
    }
}