using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Suggestions;

namespace Raven.Server.Smuggler.Documents.Data
{
    internal sealed class LegacyIndexDefinition
    {
        public int IndexId { get; set; }

        public string Name { get; set; }

        public IndexLockMode LockMode { get; set; }

        public int? IndexVersion { get; set; }

        public string Map
        {
            get => Maps.FirstOrDefault();
            set
            {
                if (Maps.Count != 0)
                {
                    Maps.Remove(Maps.First());
                }
                Maps.Add(value);
            }
        }

        public HashSet<string> Maps
        {
            get => _maps ?? (_maps = new HashSet<string>());
            set => _maps = value;
        }

        public string Reduce { get; set; }

        public Dictionary<string, FieldStorage> Stores
        {
            get => _stores ?? (_stores = new Dictionary<string, FieldStorage>());
            set => _stores = value;
        }

        public Dictionary<string, LegacyFieldIndexing> Indexes
        {
            get => _indexes ?? (_indexes = new Dictionary<string, LegacyFieldIndexing>());
            set => _indexes = value;
        }

        public Dictionary<string, LegacySortOptions> SortOptions
        {
            get => _sortOptions ?? (_sortOptions = new Dictionary<string, LegacySortOptions>());
            set => _sortOptions = value;
        }

        public Dictionary<string, string> Analyzers
        {
            get => _analyzers ?? (_analyzers = new Dictionary<string, string>());
            set => _analyzers = value;
        }

        public List<string> Fields
        {
            get => _fields ?? (_fields = new List<string>());
            set => _fields = value;
        }

        [Obsolete("Use SuggestionsOptions")]
        public Dictionary<string, SuggestionOptions> Suggestions
        {
            get
            {
                if (SuggestionsOptions == null || SuggestionsOptions.Count == 0)
                    return null;

                return SuggestionsOptions.ToDictionary(x => x, x => new SuggestionOptions());
            }
            set
            {
                if (value == null)
                    return;
                SuggestionsOptions = value.Keys.ToHashSet();
            }
        }

        public HashSet<string> SuggestionsOptions
        {
            get => _suggestionsOptions ?? (_suggestionsOptions = new HashSet<string>());
            set => _suggestionsOptions = value;
        }

        public Dictionary<string, FieldTermVector> TermVectors
        {
            get => _termVectors ?? (_termVectors = new Dictionary<string, FieldTermVector>());
            set => _termVectors = value;
        }

        public Dictionary<string, SpatialOptions> SpatialIndexes
        {
            get => _spatialIndexes ?? (_spatialIndexes = new Dictionary<string, SpatialOptions>());
            set => _spatialIndexes = value;
        }

        [IgnoreDataMember]
        private HashSet<string> _maps;
        [IgnoreDataMember]
        private Dictionary<string, FieldStorage> _stores;
        [IgnoreDataMember]
        private Dictionary<string, LegacyFieldIndexing> _indexes;
        [IgnoreDataMember]
        private Dictionary<string, LegacySortOptions> _sortOptions;
        [IgnoreDataMember]
        private Dictionary<string, string> _analyzers;
        [IgnoreDataMember]
        private List<string> _fields;
        [IgnoreDataMember]
        private Dictionary<string, FieldTermVector> _termVectors;
        [IgnoreDataMember]
        private Dictionary<string, SpatialOptions> _spatialIndexes;
        [IgnoreDataMember]
        private HashSet<string> _suggestionsOptions;

        public override string ToString()
        {
            return Name ?? Map;
        }

        public enum LegacySortOptions
        {
            None = 0,
            String = 3,
            Int = 4,
            Float = 5,
            Long = 6,
            Double = 7,
            Short = 8,
            Custom = 9,
            Byte = 10,
            StringVal = 11
        }

        public enum LegacyFieldIndexing
        {
            No,
            Analyzed,
            NotAnalyzed,
            Default
        }
    }
}
