using System;

using Raven.NewClient.Abstractions.Data;

namespace Raven.NewClient.Abstractions.Indexing
{
    public class IndexReplaceDocument
    {
        public string IndexToReplace { get; set; }

        public long? MinimumEtagBeforeReplace { get; set; }

        public DateTime? ReplaceTimeUtc { get; set; }
    }
}
