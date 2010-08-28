using System;
using System.Collections.Generic;
using Raven.Client.Client;

namespace Raven.Client.Document
{
    public class MultiTypeHiLoKeyGenerator
    {
		private readonly IDocumentStore documentStore;
        private readonly int capacity;
        private readonly object generatorLock = new object();
        private IDictionary<string, HiLoKeyGenerator> keyGeneratorsByTag = new Dictionary<string, HiLoKeyGenerator>();

        public MultiTypeHiLoKeyGenerator(IDocumentStore documentStore, int capacity)
        {
            this.documentStore = documentStore;
            this.capacity = capacity;
        }

        public string GenerateDocumentKey(DocumentConvention conventions, object entity)
        {
            var tag = conventions.GetTypeTagName(entity.GetType()).ToLowerInvariant();
            HiLoKeyGenerator value;
            if (keyGeneratorsByTag.TryGetValue(tag, out value))
				return value.GenerateDocumentKey(conventions, entity);

            lock(generatorLock)
            {
                if (keyGeneratorsByTag.TryGetValue(tag, out value))
                    return value.GenerateDocumentKey(conventions, entity);

                value = new HiLoKeyGenerator(documentStore, tag, capacity);
                // doing it this way for thread safety
                keyGeneratorsByTag = new Dictionary<string, HiLoKeyGenerator>(keyGeneratorsByTag)
                {
                    {tag, value}
                };
            }

			return value.GenerateDocumentKey(conventions, entity);
        }
    }
}