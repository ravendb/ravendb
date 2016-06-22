//-----------------------------------------------------------------------
// <copyright file="IndexDefinition.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Indexing
{
    /// <summary>
    /// A definition of a RavenIndex
    /// </summary>
    public class IndexDefinition
    {
        /// <summary>
        /// Index identifier (internal).
        /// </summary>
        public int IndexId { get; set; }

        /// <summary>
        /// This is the means by which the outside world refers to this index definition
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Index lock mode:
        /// <para>- Unlock - all index definition changes acceptable</para>
        /// <para>- LockedIgnore - all index definition changes will be ignored, only log entry will be created</para>
        /// <para>- LockedError - all index definition changes will raise exception</para>
        /// <para>- SideBySide - all index definition changes will raise exception except when updated by a side by side index</para>
        /// </summary>
        public IndexLockMode LockMode { get; set; }

        /// <summary>
        /// Index map function, if there is only one
        /// </summary>
        /// <remarks>
        /// This property only exists for backward compatibility purposes
        /// </remarks>
        public string Map
        {
            get { return Maps.FirstOrDefault(); }
            set
            {
                if (Maps.Count != 0)
                {
                    Maps.Remove(Maps.First());
                }
                Maps.Add(value);
            }
        }

        /// <summary>
        /// All the map functions for this index
        /// </summary>
        public HashSet<string> Maps
        {
            get { return maps ?? (maps = new HashSet<string>()); }
            set { maps = value; }
        }

        /// <summary>
        /// Index reduce function
        /// </summary>
        public string Reduce { get; set; }

        /// <summary>
        /// Gets a value indicating whether this instance is map reduce index definition
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is map reduce; otherwise, <c>false</c>.
        /// </value>
        public bool IsMapReduce
        {
            get { return string.IsNullOrEmpty(Reduce) == false; }
        }

        /// <summary>
        /// Internal use only.
        /// </summary>
        public bool IsCompiled { get; set; }

        /// <summary>
        /// Index field storage settings.
        /// </summary>
        public IDictionary<string, FieldStorage> Stores
        {
            get { return stores ?? (stores = new Dictionary<string, FieldStorage>()); }
            set { stores = value; }
        }

        /// <summary>
        /// Index field indexing settings.
        /// </summary>
        public IDictionary<string, FieldIndexing> Indexes
        {
            get { return indexes ?? (indexes = new Dictionary<string, FieldIndexing>()); }
            set { indexes = value; }
        }

        /// <summary>
        /// Index field sorting settings.
        /// </summary>
        public IDictionary<string, SortOptions> SortOptions
        {
            get { return sortOptions ?? (sortOptions = new Dictionary<string, SortOptions>()); }
            set { sortOptions = value; }
        }

        /// <summary>
        /// Index field analyzer settings.
        /// </summary>
        public IDictionary<string, string> Analyzers
        {
            get { return analyzers ?? (analyzers = new Dictionary<string, string>()); }
            set { analyzers = value; }
        }

        /// <summary>
        /// List of queryable fields in index.
        /// </summary>
        public IList<string> Fields
        {
            get { return fields ?? (fields = new List<string>()); }
            set { fields = value; }
        }

        /// <summary>
        /// Index field suggestion settings.
        /// </summary>
        public IDictionary<string, SuggestionOptions> Suggestions
        {
            get { return suggestions ?? (suggestions = new Dictionary<string, SuggestionOptions>()); }
            set { suggestions = value; }
        }

        /// <summary>
        /// Index field term vector settings.
        /// </summary>
        public IDictionary<string, FieldTermVector> TermVectors
        {
            get { return termVectors ?? (termVectors = new Dictionary<string, FieldTermVector>()); }
            set { termVectors = value; }
        }

        /// <summary>
        /// Index field spatial settings.
        /// </summary>
        public IDictionary<string, SpatialOptions> SpatialIndexes
        {
            get { return spatialIndexes ?? (spatialIndexes = new Dictionary<string, SpatialOptions>()); }
            set { spatialIndexes = value; }
        }

        /// <summary>
        /// Internal map of field names to expressions generating them
        /// Only relevant for auto indexes and only used internally
        /// </summary>
        public IDictionary<string, string> InternalFieldsMapping
        {
            get { return internalFieldsMapping ?? (internalFieldsMapping = new Dictionary<string, string>()); }
            set { internalFieldsMapping = value; }
        }

        /// <summary>
        /// Index specific setting that limits the number of map outputs that an index is allowed to create for a one source document. If a map operation applied to
        /// the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document 
        /// will be skipped and the appropriate error message will be added to the indexing errors.
        /// <para>Default value: null means that the global value from Raven configuration will be taken to detect if number of outputs was exceeded.</para>
        /// </summary>
        public int? MaxIndexOutputsPerDocument { get; set; }

        /// <summary>
        /// Equals the specified other.
        /// </summary>
        /// <param name="other">The other.</param>
        /// <param name="compareIndexIds">allow caller to choose whether to include the index Id in the comparison</param>
        /// <returns></returns>
        public bool Equals(IndexDefinition other, bool compareIndexIds = true, bool ignoreFormatting = false, bool ignoreMaxIndexOutput = false)
        {
            if (ReferenceEquals(null, other))
                return false;

            if (ReferenceEquals(this, other))
                return true;

            if (compareIndexIds && !Equals(other.IndexId, IndexId))
                return false;

            bool mapsReduceEquals;
            if (ignoreFormatting)
            {
#if !DNXCORE50
                var comparer = new IndexPrettyPrinterEqualityComparer();
                mapsReduceEquals = Maps.SequenceEqual(other.Maps, comparer) && comparer.Equals(Reduce, other.Reduce);
#else
                mapsReduceEquals = Maps.SequenceEqual(other.Maps) && Reduce.Equals(other.Reduce);
#endif
            }
            else
            {
                mapsReduceEquals = Maps.SequenceEqual(other.Maps) && Equals(other.Reduce, Reduce);
            }

            return mapsReduceEquals &&
                    (ignoreMaxIndexOutput || other.MaxIndexOutputsPerDocument == MaxIndexOutputsPerDocument) &&
                    DictionaryExtensions.ContentEquals(other.Stores, Stores) &&
                    DictionaryExtensions.ContentEquals(other.Indexes, Indexes) &&
                    DictionaryExtensions.ContentEquals(other.Analyzers, Analyzers) &&
                    DictionaryExtensions.ContentEquals(other.SortOptions, SortOptions) &&
                    DictionaryExtensions.ContentEquals(other.Suggestions, Suggestions) &&
                    DictionaryExtensions.ContentEquals(other.TermVectors, TermVectors) &&
                    DictionaryExtensions.ContentEquals(other.SpatialIndexes, SpatialIndexes);
        }

        private static int DictionaryHashCode<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> x)
        {
            int result = 0;
            foreach (var kvp in x)
            {
                result = (result * 397) ^ kvp.Key.GetHashCode();
                result = (result * 397) ^ (!Equals(kvp.Value, default(TValue)) ? kvp.Value.GetHashCode() : 0);
            }
            return result;
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
        /// <returns>
        /// 	<c>true</c> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            return Equals(obj as IndexDefinition);
        }

        [JsonIgnore]
        private byte[] cachedHashCodeAsBytes;
        [JsonIgnore]
        private HashSet<string> maps;
        [JsonIgnore]
        private IDictionary<string, FieldStorage> stores;
        [JsonIgnore]
        private IDictionary<string, FieldIndexing> indexes;
        [JsonIgnore]
        private IDictionary<string, SortOptions> sortOptions;
        [JsonIgnore]
        private IDictionary<string, string> analyzers;
        [JsonIgnore]
        private IList<string> fields;
        [JsonIgnore]
        private IDictionary<string, SuggestionOptions> suggestions;
        [JsonIgnore]
        private IDictionary<string, FieldTermVector> termVectors;
        [JsonIgnore]
        private IDictionary<string, SpatialOptions> spatialIndexes;
        [JsonIgnore]
        private IDictionary<string, string> internalFieldsMapping;

        /// <summary>
        /// Provide a cached version of the index hash code, which is used when generating
        /// the index etag. 
        /// It isn't really useful for anything else, in particular, we cache that because
        /// we want to avoid calculating the cost of doing this over and over again on each 
        /// query.
        /// </summary>
        public byte[] GetIndexHash()
        {
            if (cachedHashCodeAsBytes != null)
                return cachedHashCodeAsBytes;

            cachedHashCodeAsBytes = BitConverter.GetBytes(GetHashCode());
            return cachedHashCodeAsBytes;
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int result = Maps.Where(x => x != null).Aggregate(0, (acc, val) => acc * 397 ^ val.GetHashCode());
                result = (result * 397) ^ Maps.Count;
                result = (result * 397) ^ (Reduce != null ? Reduce.GetHashCode() : 0);
                result = (result * 397) ^ DictionaryHashCode(Stores);
                result = (result * 397) ^ DictionaryHashCode(Indexes);
                result = (result * 397) ^ DictionaryHashCode(Analyzers);
                result = (result * 397) ^ DictionaryHashCode(SortOptions);
                result = (result * 397) ^ DictionaryHashCode(Suggestions);
                result = (result * 397) ^ DictionaryHashCode(TermVectors);
                result = (result * 397) ^ DictionaryHashCode(SpatialIndexes);
                return result;
            }
        }

        public string Type
        {
            get
            {
                var name = Name ?? string.Empty;
                if (name.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase))
                    return "Auto";
                if (IsCompiled)
                    return "Compiled";
                if (IsMapReduce)
                    return "MapReduce";
                return "Map";
            }
        }

        /// <summary>
        /// Prevent index from being kept in memory. Default: false
        /// </summary>
        public bool DisableInMemoryIndexing { get; set; }

        /// <summary>
        /// Whatever this is a temporary test only index
        /// </summary>
        public bool IsTestIndex { get; set; }

        /// <summary>
        /// Whatever this is a side by side index
        /// </summary>
        public bool IsSideBySideIndex { get; set; }

        /// <summary>
        /// Remove the default values that we don't actually need
        /// </summary>
        public void RemoveDefaultValues()
        {
            const FieldStorage defaultStorage = FieldStorage.No;
            foreach (var toRemove in Stores.Where(x => x.Value == defaultStorage).ToArray())
            {
                Stores.Remove(toRemove);
            }
            foreach (var toRemove in Indexes.Where(x => x.Value == FieldIndexing.Default).ToArray())
            {
                Indexes.Remove(toRemove);
            }
            foreach (var toRemove in SortOptions.Where(x => x.Value == Indexing.SortOptions.None).ToArray())
            {
                SortOptions.Remove(toRemove);
            }
            foreach (var toRemove in Analyzers.Where(x => string.IsNullOrEmpty(x.Value)).ToArray())
            {
                Analyzers.Remove(toRemove);
            }
            foreach (var toRemove in Suggestions.Where(x => x.Value.Distance == StringDistanceTypes.None).ToArray())
            {
                Suggestions.Remove(toRemove);
            }
            foreach (var toRemove in TermVectors.Where(x => x.Value == FieldTermVector.No).ToArray())
            {
                TermVectors.Remove(toRemove);
            }
        }

        public override string ToString()
        {
            return Name ?? Map;
        }
    }
}
