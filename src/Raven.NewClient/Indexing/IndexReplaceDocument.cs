using System;

using Raven.Abstractions.Data;

namespace Raven.Abstractions.Indexing
{
    public class IndexReplaceDocument
    {
        public string IndexToReplace { get; set; }

        public long? MinimumEtagBeforeReplace { get; set; }

        public DateTime? ReplaceTimeUtc { get; set; }
    }
}
