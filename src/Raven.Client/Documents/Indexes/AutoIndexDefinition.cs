using System.Collections.Generic;

namespace Raven.Client.Documents.Indexes
{
    public class AutoIndexDefinition
    {
        public IndexType Type { get; set; }

        public long Etag { get; set; }

        public string Name { get; set; }

        public IndexPriority? Priority { get; set; }

        public IndexLockMode? LockMode { get; set; }

        public string Collection { get; set; }

        public Dictionary<string, AutoIndexFieldOptions> MapFields { get; set; }

        public Dictionary<string, AutoIndexFieldOptions> GroupByFields { get; set; }

        public class AutoIndexFieldOptions : IndexFieldOptions
        {
            public FieldMapReduceOperation MapReduceOperation { get; set; }
        }
    }
}
