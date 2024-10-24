//-----------------------------------------------------------------------
// <copyright file="IndexDefinition.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// A definition of a RavenIndex
    /// </summary>
    public class IndexDefinition : IndexDefinitionBase
    {
        private static readonly Regex _newLineCharacters = new Regex("\r?\n", RegexOptions.Compiled);

        public IndexDefinition()
        {
            _configuration = new IndexConfiguration();
        }

        /// <summary>
        /// Index lock mode:
        /// <para>- Unlock - all index definition changes acceptable</para>
        /// <para>- LockedIgnore - all index definition changes will be ignored, only log entry will be created</para>
        /// <para>- LockedError - all index definition changes will raise exception</para>
        /// </summary>
        public IndexLockMode? LockMode { get; set; }

        /// <summary>
        /// Additional code files to be compiled with this index.
        /// </summary>
        public Dictionary<string, string> AdditionalSources
        {
            get => _additionalSources ??= new Dictionary<string, string>();
            set => _additionalSources = value;
        }

        /// <summary>
        /// Expert:
        /// List of compound fields that can be used by the Corax indexing engine to
        /// optimize certain queries. Has no effect on Lucene indexes. 
        /// </summary>
        public List<string[]> CompoundFields
        {
            get => _compoundFields ??= new List<string[]>();
            set => _compoundFields = value;
        }

        public HashSet<AdditionalAssembly> AdditionalAssemblies
        {
            get => _additionalAssemblies ??= new HashSet<AdditionalAssembly>();
            set => _additionalAssemblies = value;
        }

        /// <summary>
        /// All the map functions for this index
        /// </summary>
        public HashSet<string> Maps
        {
            get => _maps ?? (_maps = new HashSet<string>());
            set => _maps = value;
        }

        /// <summary>
        /// Index reduce function
        /// </summary>
        public string Reduce { get; set; }

        public Dictionary<string, IndexFieldOptions> Fields
        {
            get => _fields ?? (_fields = new Dictionary<string, IndexFieldOptions>());
            set => _fields = value;
        }

        public IndexConfiguration Configuration
        {
            get => _configuration ?? (_configuration = new IndexConfiguration());
            set => _configuration = value;
        }

        private IndexSourceType? _indexSourceType;

        public virtual IndexSourceType SourceType
        {
            get
            {
                if (_indexSourceType == null || _indexSourceType.Value == IndexSourceType.None)
                {
                    _indexSourceType = DetectStaticIndexSourceType();
                }

                return _indexSourceType.Value;
            }
            internal set => _indexSourceType = value;
        }

        public virtual ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }

        public IndexDefinitionCompareDifferences Compare(IndexDefinition other)
        {
            // TODO arek need to compare other.SourceType ?

            if (other == null)
                throw new ArgumentNullException(nameof(other));

            var result = IndexDefinitionCompareDifferences.None;

            if (ReferenceEquals(this, other))
                return result;

            if (Maps.SetEquals(other.Maps) == false)
            {
                var maps = new HashSet<string>();
                foreach (var map in Maps)
                    maps.Add(Normalize(map));

                var otherMaps = new HashSet<string>();
                foreach (var map in other.Maps)
                    otherMaps.Add(Normalize(map));

                if (maps.SetEquals(otherMaps) == false)
                    result |= IndexDefinitionCompareDifferences.Maps;
            }

            if (Equals(Reduce, other.Reduce) == false)
            {
                var reduce = Normalize(Reduce);
                var otherReduce = Normalize(other.Reduce);

                if (Equals(reduce, otherReduce) == false)
                    result |= IndexDefinitionCompareDifferences.Reduce;
            }

            if (DictionaryExtensions.ContentEquals(other.Fields, Fields) == false) //compareNullValues is false because CopyTo doesnt copy kvp's with null values.
                result |= IndexDefinitionCompareDifferences.Fields;

            bool configurationEquals;
            if (other._configuration == null && _configuration == null)
                configurationEquals = true;
            else if (other._configuration != null)
                configurationEquals = other._configuration.Equals(_configuration);
            else
                configurationEquals = _configuration.Equals(other._configuration);

            if (configurationEquals == false)
                result |= IndexDefinitionCompareDifferences.Configuration;

            if (string.Equals(OutputReduceToCollection, other.OutputReduceToCollection, StringComparison.OrdinalIgnoreCase) == false)
                result |= IndexDefinitionCompareDifferences.Reduce;

            if (string.Equals(PatternForOutputReduceToCollectionReferences, other.PatternForOutputReduceToCollectionReferences, StringComparison.OrdinalIgnoreCase) == false)
                result |= IndexDefinitionCompareDifferences.Reduce;

            if (string.Equals(PatternReferencesCollectionName, other.PatternReferencesCollectionName, StringComparison.OrdinalIgnoreCase) == false)
                result |= IndexDefinitionCompareDifferences.Reduce;

            if (LockMode != other.LockMode)
            {
                if ((LockMode == null && other.LockMode == IndexLockMode.Unlock) || (LockMode == IndexLockMode.Unlock && other.LockMode == null))
                {
                    // same
                }
                else
                {
                    result |= IndexDefinitionCompareDifferences.LockMode;
                }
            }
            
            if (ArchivedDataProcessingBehavior != other.ArchivedDataProcessingBehavior)
            {
                result |= IndexDefinitionCompareDifferences.ArchivedDataProcessingBehavior;
            }

            if (Priority != other.Priority)
            {
                if ((Priority == null && other.Priority == IndexPriority.Normal) || (Priority == IndexPriority.Normal && other.Priority == null))
                {
                    // same
                }
                else
                {
                    result |= IndexDefinitionCompareDifferences.Priority;
                }
            }

            if (State != other.State)
            {
                if ((State == null && other.State == IndexState.Normal) || (State == IndexState.Normal && other.State == null))
                {
                    // same
                }
                else
                {
                    result |= IndexDefinitionCompareDifferences.State;
                }
            }

            if (DeploymentMode != other.DeploymentMode)
            {
                if (other.DeploymentMode == null)
                {
                    // same
                }
                else
                {
                    result |= IndexDefinitionCompareDifferences.DeploymentMode;
                }
            }

            if (CompoundFields.Count != other.CompoundFields.Count)
                result |= IndexDefinitionCompareDifferences.CompoundFields;
            else if (CompoundFields.Count > 0)
            {
                foreach (var compoundField in CompoundFields)
                {
                    var matched = other.CompoundFields.Count(i => i.Length == compoundField.Length && compoundField.SequenceEqual(i, StringComparer.InvariantCulture)) > 0;
                    if (matched is false)
                    {
                        result |= IndexDefinitionCompareDifferences.CompoundFields;
                        break;
                    }
                }
            }
            
            
            if (DictionaryExtensions.ContentEquals(AdditionalSources, other.AdditionalSources) == false)
            {
                var additionalSources = new Dictionary<string, string>();
                foreach (var kvp in AdditionalSources)
                    additionalSources[kvp.Key] = Normalize(kvp.Value);

                var otherAdditionalSources = new Dictionary<string, string>();
                foreach (var kvp in other.AdditionalSources)
                    otherAdditionalSources[kvp.Key] = Normalize(kvp.Value);

                if (DictionaryExtensions.ContentEquals(additionalSources, otherAdditionalSources) == false)
                    result |= IndexDefinitionCompareDifferences.AdditionalSources;
            }

            bool additionalAssembliesEquals;
            if (AdditionalAssemblies == null && other.AdditionalAssemblies == null)
                additionalAssembliesEquals = true;
            else if (AdditionalAssemblies != null && other.AdditionalAssemblies != null)
                additionalAssembliesEquals = AdditionalAssemblies.SetEquals(other.AdditionalAssemblies);
            else
                additionalAssembliesEquals = false;

            if (additionalAssembliesEquals == false)
                result |= IndexDefinitionCompareDifferences.AdditionalAssemblies;

            return result;

            static string Normalize(string toNormalize)
            {
                if (string.IsNullOrEmpty(toNormalize))
                    return toNormalize;

                return _newLineCharacters.Replace(toNormalize, Environment.NewLine);
            }
        }

        /// <summary>
        /// Equals the specified other.
        /// </summary>
        /// <param name="other">The other.</param>
        public bool Equals(IndexDefinition other)
        {
            if (ReferenceEquals(null, other))
                return false;

            if (ReferenceEquals(this, other))
                return true;

            var result = Compare(other);

            if (result == IndexDefinitionCompareDifferences.None)
                return true;

            var mapsReduceEquals = result.HasFlag(IndexDefinitionCompareDifferences.Maps) == false && result.HasFlag(IndexDefinitionCompareDifferences.Reduce) == false;

            var configurationEquals = result.HasFlag(IndexDefinitionCompareDifferences.Configuration) == false;
            var fieldsEquals = result.HasFlag(IndexDefinitionCompareDifferences.Fields) == false;

            return mapsReduceEquals
                   && configurationEquals
                   && fieldsEquals;
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
        private byte[] _cachedHashCodeAsBytes;

        [JsonIgnore]
        private HashSet<string> _maps;

        [JsonIgnore]
        private Dictionary<string, IndexFieldOptions> _fields;

        [JsonIgnore]
        private Dictionary<string, string> _additionalSources;

        [JsonIgnore]
        private HashSet<AdditionalAssembly> _additionalAssemblies;

        [JsonIgnore]
        private IndexConfiguration _configuration;

        [JsonIgnore]
        private List<string[]> _compoundFields;

        /// <summary>
        /// Provide a cached version of the index hash code, which is used when generating
        /// the index etag.
        /// It isn't really useful for anything else, in particular, we cache that because
        /// we want to avoid calculating the cost of doing this over and over again on each
        /// query.
        /// </summary>
        public byte[] GetIndexHash()
        {
            if (_cachedHashCodeAsBytes != null)
                return _cachedHashCodeAsBytes;

            _cachedHashCodeAsBytes = BitConverter.GetBytes(GetHashCode());
            return _cachedHashCodeAsBytes;
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
                var result = Maps.GetEnumerableHashCode();
                result = (result * 397) ^ Maps.Count;
                result = (result * 397) ^ (Reduce?.GetHashCode() ?? 0);
                result = (result * 397) ^ DictionaryHashCode(Fields);
                result = (result * 397) ^ DictionaryHashCode(AdditionalSources);
                result = (result * 397) ^ AdditionalAssemblies.Count;
                result = (result * 397) ^ AdditionalAssemblies.GetEnumerableHashCode();
                result = (result * 397) ^ (OutputReduceToCollection?.GetHashCode() ?? 0);
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
            internal set => _indexType = value;
        }

        /// <summary>
        /// Remove the default values that we don't actually need
        /// </summary>
        public void RemoveDefaultValues()
        {
            // do not remove default values if we have default field options specified
            if (Fields.ContainsKey(Constants.Documents.Indexing.Fields.AllFields))
                return;

            var toRemove = new List<string>();
            foreach (var kvp in Fields)
            {
                var allDefault = true;
                var field = kvp.Value;

                if (field != null)
                {
                    if (field.Storage.HasValue)
                    {
                        if (field.Storage == FieldStorage.No)
                            field.Storage = null;
                        else
                            allDefault = false;
                    }

                    if (field.Indexing.HasValue)
                    {
                        if (field.Indexing == FieldIndexing.Default)
                            field.Indexing = null;
                        else
                            allDefault = false;
                    }

                    if (string.IsNullOrWhiteSpace(field.Analyzer))
                        field.Analyzer = null;
                    else
                        allDefault = false;

                    if (field.TermVector.HasValue)
                    {
                        if (field.TermVector == FieldTermVector.No)
                            field.TermVector = null;
                        else
                            allDefault = false;
                    }

                    if (field.Suggestions.HasValue)
                    {
                        if (field.Suggestions == false)
                            field.Suggestions = null;
                        else
                            allDefault = false;
                    }

                    allDefault = allDefault && (field.Spatial == null) && (field.Vector == null);
                }

                if (allDefault)
                    toRemove.Add(kvp.Key);
            }

            foreach (var key in toRemove)
                Fields.Remove(key);
        }

        public IndexSourceType DetectStaticIndexSourceType()
        {
            if (Maps == null || Maps.Count == 0)
                throw new ArgumentNullException("Index definition contains no Maps");

            var sourceType = IndexSourceType.None;
            foreach (var map in Maps)
            {
                var mapSourceType = IndexDefinitionHelper.DetectStaticIndexSourceType(map);
                if (sourceType == IndexSourceType.None)
                {
                    sourceType = mapSourceType;
                    continue;
                }

                if (sourceType != mapSourceType)
                    throw new InvalidOperationException("Index definition cannot contain Maps with different source types.");
            }

            return sourceType;
        }

        public IndexType DetectStaticIndexType()
        {
            var firstMap = Maps.FirstOrDefault();
            if (firstMap == null)
                throw new ArgumentNullException("Index definitions contains no Maps");

            return IndexDefinitionHelper.DetectStaticIndexType(firstMap, Reduce);
        }

#if FEATURE_TEST_INDEX
        /// <summary>
        /// Whether this is a temporary test only index
        /// </summary>
        public bool IsTestIndex { get; set; }
#endif

        /// <summary>
        /// If not null than each reduce result will be created as a document in the specified collection name.
        /// </summary>
        public string OutputReduceToCollection { get; set; }

        /// <summary>
        /// If not null then this number will be part of identifier of a created document being output of reduce function
        /// </summary>
        public long? ReduceOutputIndex { get; set; }

        /// <summary>
        /// Defines pattern for identifiers of documents which reference IDs of reduce outputs documents
        /// </summary>
        public string PatternForOutputReduceToCollectionReferences { get; set; }

        /// <summary>
        /// Defines a collection name for reference documents created based on provided pattern
        /// </summary>
        public string PatternReferencesCollectionName { get; set; }

        /// <summary>
        /// Define index deployment mode
        /// </summary>
        public IndexDeploymentMode? DeploymentMode { get; set; }

        public override string ToString()
        {
            return Name;
        }

        internal void CopyTo(IndexDefinition definition)
        {
            Dictionary<string, IndexFieldOptions> fields = null;
            if (_fields != null)
            {
                fields = new Dictionary<string, IndexFieldOptions>();

                foreach (var kvp in _fields)
                {
                    var value = kvp.Value;
                    if (value == null)
                        continue;

                    fields[kvp.Key] = new IndexFieldOptions
                    {
                        Indexing = value.Indexing,
                        Analyzer = value.Analyzer,
                        Spatial = value.Spatial,
                        Storage = value.Storage,
                        Suggestions = value.Suggestions,
                        TermVector = value.TermVector,
                        Vector = value.Vector
                    };
                }
            }

            definition.LockMode = LockMode;
            definition.ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior;
            definition.Fields = fields;
            definition.Name = Name;
            definition.Priority = Priority;
            definition.Reduce = Reduce;
            definition.Maps = new HashSet<string>(Maps);
            definition.Configuration = new IndexConfiguration();
            definition.State = State;
#if FEATURE_TEST_INDEX
                definition.IsTestIndex = IsTestIndex;
#endif
            definition.OutputReduceToCollection = OutputReduceToCollection;
            definition.ReduceOutputIndex = ReduceOutputIndex;
            definition.PatternForOutputReduceToCollectionReferences = PatternForOutputReduceToCollectionReferences;
            definition.PatternReferencesCollectionName = PatternReferencesCollectionName;
            definition.DeploymentMode = DeploymentMode;
            definition.ClusterState = (ClusterState == null) ? null : new IndexDefinitionClusterState(ClusterState);
            definition.CompoundFields = CompoundFields;

            foreach (var kvp in _configuration)
                definition.Configuration[kvp.Key] = kvp.Value;

            if (_additionalSources != null)
            {
                foreach (var kvp in _additionalSources)
                    definition.AdditionalSources[kvp.Key] = kvp.Value;
            }

            if (_additionalAssemblies != null)
            {
                foreach (var additionalAssembly in _additionalAssemblies)
                    definition.AdditionalAssemblies.Add(additionalAssembly.Clone());
            }
        }

        internal static readonly IndexDefinitionCompareDifferences ReIndexRequiredMask =
            IndexDefinitionCompareDifferences.Maps | 
            IndexDefinitionCompareDifferences.Reduce | 
            IndexDefinitionCompareDifferences.Fields | 
            IndexDefinitionCompareDifferences.Configuration | 
            IndexDefinitionCompareDifferences.AdditionalSources | 
            IndexDefinitionCompareDifferences.AdditionalAssemblies;
    }

    public enum IndexDeploymentMode
    {
        Parallel,
        Rolling
    }

    /// <summary>
    /// Defines the modes for resetting an index.
    /// </summary>
    /// <value>
    /// <list type="bullet">
    /// <item>
    /// <term>InPlace</term>
    /// <description>Resets the index in place, replacing the existing index with the newly rebuilt one.</description>
    /// </item>
    /// <item>
    /// <term>SideBySide</term>
    /// <description>Resets the index in a side-by-side manner, allowing the new index to be built alongside the existing one.</description>
    /// </item>
    /// </list>
    /// </value>
    public enum IndexResetMode
    {
        InPlace,
        SideBySide
    }

    [Flags]
    public enum IndexDefinitionCompareDifferences
    {
        None = 0,
        Maps = 1 << 1,
        Reduce = 1 << 3,
        Fields = 1 << 5,
        Configuration = 1 << 6,
        LockMode = 1 << 7,
        Priority = 1 << 8,
        State = 1 << 9,
        AdditionalSources = 1 << 10,
        AdditionalAssemblies = 1 << 11,
        DeploymentMode = 1 << 12,
        CompoundFields = 1 << 13,
        ArchivedDataProcessingBehavior = 1 << 14,

        All = Maps | Reduce | Fields | Configuration | LockMode | Priority | State | AdditionalSources | AdditionalAssemblies | DeploymentMode | CompoundFields | ArchivedDataProcessingBehavior,
    }
}
