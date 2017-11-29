using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Suggestions;

namespace Raven.Server.Smuggler.Documents.Data
{
    internal class LegacyIndexDefinition
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

        [JsonIgnore]
        private HashSet<string> _maps;
        [JsonIgnore]
        private Dictionary<string, FieldStorage> _stores;
        [JsonIgnore]
        private Dictionary<string, LegacyFieldIndexing> _indexes;
        [JsonIgnore]
        private Dictionary<string, LegacySortOptions> _sortOptions;
        [JsonIgnore]
        private Dictionary<string, string> _analyzers;
        [JsonIgnore]
        private List<string> _fields;
        [JsonIgnore]
        private Dictionary<string, FieldTermVector> _termVectors;
        [JsonIgnore]
        private Dictionary<string, SpatialOptions> _spatialIndexes;
        [JsonIgnore]
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
