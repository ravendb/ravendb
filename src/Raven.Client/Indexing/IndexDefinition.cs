//-----------------------------------------------------------------------
// <copyright file="IndexDefinition.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client.Data.Indexes;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.Indexing
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
        /// </summary>
        public IndexLockMode LockMode { get; set; }

        /// <summary>
        /// Index version, used in index replication in order to identify if two indexes are indeed the same.
        /// </summary>
        public int? IndexVersion { get; set; }

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

        public Dictionary<string, IndexFieldOptions> Fields
        {
            get { return fields ?? (fields = new Dictionary<string, IndexFieldOptions>()); }
            set { fields = value; }
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
        /// <param name="ignoreFormatting">Comparision ignores formatting in both of the definitions</param>
        /// <param name="ignoreMaxIndexOutput">Comparision ignores MaxIndexOutputsPerDocument</param>
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
                var comparer = new IndexPrettyPrinterEqualityComparer();
                mapsReduceEquals = Maps.SequenceEqual(other.Maps, comparer) && comparer.Equals(Reduce, other.Reduce);
            }
            else
            {
                mapsReduceEquals = Maps.SequenceEqual(other.Maps) && Equals(other.Reduce, Reduce);
            }

            return mapsReduceEquals
                   && (ignoreMaxIndexOutput || other.MaxIndexOutputsPerDocument == MaxIndexOutputsPerDocument)
                   && DictionaryExtensions.ContentEquals(other.Fields, Fields);
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
        private Dictionary<string, IndexFieldOptions> fields;

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
                result = (result * 397) ^ (Reduce?.GetHashCode() ?? 0);
                result = (result * 397) ^ DictionaryHashCode(Fields);
                return result;
            }
        }

        private IndexType? _indexType;

        public IndexType Type
        {
            get
            {
                if (_indexType == null || _indexType.Value == IndexType.None)
                {
                    _indexType = DetectStaticIndexType();
                }

                return _indexType.Value;
            }
            internal set
            {
                _indexType = value;
            }
        }

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
            var toRemove = new List<string>();
            foreach (var kvp in Fields)
            {
                var allDefault = true;
                var field = kvp.Value;

                if (field.Storage == FieldStorage.No)
                    field.Storage = null;
                else
                    allDefault = false;

                if (field.Indexing == FieldIndexing.Default)
                    field.Indexing = null;
                else
                    allDefault = false;

                if (string.IsNullOrWhiteSpace(field.Analyzer))
                    field.Analyzer = null;
                else
                    allDefault = false;

                if (field.Sort == SortOptions.None)
                    field.Sort = null;
                else
                    allDefault = false;

                if (field.TermVector == FieldTermVector.No)
                    field.TermVector = null;
                else
                    allDefault = false;

                if (field.Suggestions == false)
                    field.Suggestions = null;
                else
                    allDefault = false;

                allDefault = allDefault && (field.Spatial == null);

                if (allDefault)
                    toRemove.Add(kvp.Key);
            }

            foreach (var key in toRemove)
                Fields.Remove(key);
        }

        private IndexType DetectStaticIndexType()
        {
            if (string.IsNullOrWhiteSpace(Reduce))
                return IndexType.Map;

            return IndexType.MapReduce;
        }

        public override string ToString()
        {
            return Name;
        }
    }
}
