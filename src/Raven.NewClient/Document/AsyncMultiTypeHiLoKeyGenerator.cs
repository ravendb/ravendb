//-----------------------------------------------------------------------
// <copyright file="MultiTypeHiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Util;


namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Generate a hilo key for each given type
    /// </summary>
    public class AsyncMultiTypeHiLoKeyGenerator
    {
        private readonly int capacity;
        private readonly object generatorLock = new object();
        private readonly ConcurrentDictionary<string, AsyncHiLoKeyGenerator> keyGeneratorsByTag = new ConcurrentDictionary<string, AsyncHiLoKeyGenerator>();


        public AsyncMultiTypeHiLoKeyGenerator(int capacity)
        {
            this.capacity = capacity;
        }

        
        public Task<string> GenerateDocumentKeyAsync(DocumentConvention conventions, object entity)
        {
            var typeTagName = conventions.GetDynamicTagName(entity);
            if (string.IsNullOrEmpty(typeTagName)) //ignore empty tags
                return CompletedTask.With<string>(null);
            var tag = conventions.TransformTypeTagNameToDocumentKeyPrefix(typeTagName);
            AsyncHiLoKeyGenerator value;
            if (keyGeneratorsByTag.TryGetValue(tag, out value))
                return value.GenerateDocumentKeyAsync(conventions, entity);

            lock(generatorLock)
            {
                if (keyGeneratorsByTag.TryGetValue(tag, out value))
                    return value.GenerateDocumentKeyAsync(conventions, entity);

                value = new AsyncHiLoKeyGenerator(tag, capacity);
                keyGeneratorsByTag.TryAdd(tag, value);
            }

            return value.GenerateDocumentKeyAsync(conventions, entity);
        }
    }
}
