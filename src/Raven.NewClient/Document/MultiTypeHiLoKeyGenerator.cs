//-----------------------------------------------------------------------
// <copyright file="MultiTypeHiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.NewClient.Client.Connection;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Generate a hilo key for each given type
    /// </summary>
    public class MultiTypeHiLoKeyGenerator
    {
        private readonly object _generatorLock = new object();
        private IDictionary<string, HiLoKeyGenerator> _keyGeneratorsByTag = new Dictionary<string, HiLoKeyGenerator>();
        private readonly DocumentStore _store;
        private readonly string _dbName;
        private readonly DocumentConvention _conventions;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiTypeHiLoKeyGenerator"/> class.
        /// </summary>
        public MultiTypeHiLoKeyGenerator(DocumentStore store, string dbName, DocumentConvention conventions)
        {
            _store = store;
            _dbName = dbName;
            _conventions = conventions;
        }

        /// <summary>
        /// Generates the document key.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public string GenerateDocumentKey(object entity)
        {
         var typeTagName = _conventions.GetDynamicTagName(entity);
            if (string.IsNullOrEmpty(typeTagName)) //ignore empty tags
                return null;
            var tag = _conventions.TransformTypeTagNameToDocumentKeyPrefix(typeTagName);
            HiLoKeyGenerator value;
            if (_keyGeneratorsByTag.TryGetValue(tag, out value))
                return value.GenerateDocumentKey(entity);

            lock(_generatorLock)
            {
                if (_keyGeneratorsByTag.TryGetValue(tag, out value))
                    return value.GenerateDocumentKey(entity);

                value = new HiLoKeyGenerator(tag, _store, _dbName, _conventions.IdentityPartsSeparator);
                // doing it this way for thread safety
                _keyGeneratorsByTag = new Dictionary<string, HiLoKeyGenerator>(_keyGeneratorsByTag)
                {
                    {tag, value}
                };
            }

            return value.GenerateDocumentKey(entity);
        }

        public void ReturnUnusedRange()
        {
            foreach (var generator in _keyGeneratorsByTag)
            {
                generator.Value.ReturnUnusedRange();
            }
        }
    }
}
