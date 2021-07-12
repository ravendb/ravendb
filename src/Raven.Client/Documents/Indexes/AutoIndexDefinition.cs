using System.Collections.Generic;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Indexes
{
    public class AutoIndexDefinition : IndexDefinitionBase
    {
        public AutoIndexDefinition()
        {
            MapFields = new Dictionary<string, AutoIndexFieldOptions>();
            GroupByFields = new Dictionary<string, AutoIndexFieldOptions>();
        }

        public IndexType Type { get; set; }

        public string Collection { get; set; }

        public Dictionary<string, AutoIndexFieldOptions> MapFields { get; set; }

        public Dictionary<string, AutoIndexFieldOptions> GroupByFields { get; set; }

        public IndexDefinitionCompareDifferences Compare(AutoIndexDefinition other)
        {
            if (other == null)
                return IndexDefinitionCompareDifferences.All;

            if (ReferenceEquals(this, other))
                return IndexDefinitionCompareDifferences.None;

            var result = IndexDefinitionCompareDifferences.None;

            if (string.Equals(Collection, other.Collection) == false || DictionaryExtensions.ContentEquals(MapFields, other.MapFields) == false)
                result |= IndexDefinitionCompareDifferences.Maps;

            if (DictionaryExtensions.ContentEquals(GroupByFields, other.GroupByFields) == false)
                result |= IndexDefinitionCompareDifferences.Reduce;

            if (Priority != other.Priority)
                result |= IndexDefinitionCompareDifferences.Priority;

            if (State != other.State)
                result |= IndexDefinitionCompareDifferences.State;

            return result;
        }

        public class AutoIndexFieldOptions
        {
            public FieldStorage? Storage { get; set; }

            public AutoFieldIndexing? Indexing { get; set; }

            public AggregationOperation Aggregation { get; set; }

            public AutoSpatialOptions Spatial { get; set; }

            public GroupByArrayBehavior GroupByArrayBehavior { get; set; }

            public bool? Suggestions { get; set; }

            public bool IsNameQuoted { get; set; }

            protected bool Equals(AutoIndexFieldOptions other)
            {
                return Storage == other.Storage
                       && Indexing == other.Indexing
                       && Aggregation == other.Aggregation
                       && Equals(Spatial, other.Spatial)
                       && GroupByArrayBehavior == other.GroupByArrayBehavior
                       && Suggestions == other.Suggestions
                       && IsNameQuoted == other.IsNameQuoted;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (ReferenceEquals(this, obj))
                    return true;
                if (obj.GetType() != GetType())
                    return false;
                return Equals((AutoIndexFieldOptions)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = Storage.GetHashCode();
                    hashCode = (hashCode * 397) ^ Indexing.GetHashCode();
                    hashCode = (hashCode * 397) ^ (int)Aggregation;
                    hashCode = (hashCode * 397) ^ (Spatial != null ? Spatial.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (int)GroupByArrayBehavior;
                    hashCode = (hashCode * 397) ^ Suggestions.GetHashCode();
                    hashCode = (hashCode * 397) ^ IsNameQuoted.GetHashCode();
                    return hashCode;
                }
            }
        }
    }
}
