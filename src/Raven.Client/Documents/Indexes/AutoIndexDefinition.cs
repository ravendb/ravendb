using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Client.Documents.Indexes
{
    public class AutoIndexDefinition
    {
        public AutoIndexDefinition()
        {
            MapFields = new Dictionary<string, AutoIndexFieldOptions>();
            GroupByFields = new Dictionary<string, AutoIndexFieldOptions>();
        }

        public IndexType Type { get; set; }

        public long Etag { get; set; }

        public string Name { get; set; }

        public IndexPriority? Priority { get; set; }

        public IndexLockMode? LockMode { get; set; }

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

            if (string.Equals(Collection, other.Collection) == false || MapFields.SequenceEqual(other.MapFields) == false)
                result |= IndexDefinitionCompareDifferences.Maps;

            if (GroupByFields.SequenceEqual(other.GroupByFields) == false)
                result |= IndexDefinitionCompareDifferences.Reduce;

            if (Priority != other.Priority)
                result |= IndexDefinitionCompareDifferences.Priority;

            if (LockMode != other.LockMode)
                result |= IndexDefinitionCompareDifferences.LockMode;

            if (Etag != other.Etag)
                result |= IndexDefinitionCompareDifferences.Etag;

            return result;
        }

        public class AutoIndexFieldOptions
        {
            public FieldStorage? Storage { get; set; }

            public AutoFieldIndexing? Indexing { get; set; }

            public AggregationOperation Aggregation { get; set; }

            public AutoSpatialOptions Spatial { get; set; }

            protected bool Equals(AutoIndexFieldOptions other)
            {
                return Storage == other.Storage && Indexing == other.Indexing && Aggregation == other.Aggregation && Equals(Spatial, other.Spatial);
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
                    return hashCode;
                }
            }
        }
    }
}
