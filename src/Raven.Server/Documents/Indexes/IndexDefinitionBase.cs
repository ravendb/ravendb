using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexDefinitionBase
    {
        protected static readonly Slice DefinitionSlice = "Definition";

        private int? _cachedHashCode;

        protected IndexDefinitionBase(string name, string[] collections, IndexLockMode lockMode, IndexField[] mapFields)
        {
            Name = name;
            Collections = collections;
            MapFields = mapFields.ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);
            LockMode = lockMode;
        }

        public string Name { get; }

        public string[] Collections { get; }

        public Dictionary<string, IndexField> MapFields { get; }

        public IndexLockMode LockMode { get; set; }

        public abstract void Persist(TransactionOperationContext context);

        public IndexDefinition ConvertToIndexDefinition(Index index)
        {
            var indexDefinition = new IndexDefinition();
            indexDefinition.IndexId = index.IndexId;
            indexDefinition.Name = index.Name;
            indexDefinition.Fields = MapFields.ToDictionary(
                x => x.Key,
                x => new IndexFieldOptions
                {
                    Sort = x.Value.SortOption,
                    TermVector = x.Value.Highlighted ? FieldTermVector.WithPositionsAndOffsets : (FieldTermVector?)null,
                    Analyzer = x.Value.Analyzer,
                    Indexing = x.Value.Indexing,
                    Storage = x.Value.Storage
                });

            indexDefinition.Type = index.Type;
            indexDefinition.LockMode = LockMode;

            indexDefinition.IndexVersion = -1; // TODO [ppekrol]      
            indexDefinition.IsSideBySideIndex = false; // TODO [ppekrol]
            indexDefinition.IsTestIndex = false; // TODO [ppekrol]       
            indexDefinition.MaxIndexOutputsPerDocument = null; // TODO [ppekrol]

            FillIndexDefinition(indexDefinition);

            return indexDefinition;
        }

        protected abstract void FillIndexDefinition(IndexDefinition indexDefinition);

        public bool ContainsField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return MapFields.ContainsKey(field);
        }

        public IndexField GetField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return MapFields[field];
        }

        public abstract bool Equals(IndexDefinitionBase indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs);
        
        public override int GetHashCode()
        {
            if (_cachedHashCode != null)
                return _cachedHashCode.Value;

            unchecked
            {
                var hashCode = MapFields?.GetDictionaryHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Name?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Collections?.GetEnumerableHashCode() ?? 0);

                _cachedHashCode = hashCode;

                return hashCode;
            }
        }
    }
}