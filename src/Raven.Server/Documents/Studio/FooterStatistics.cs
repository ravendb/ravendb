using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Studio
{
    public class FooterStatistics
    {
        public long CountOfDocuments { get; set; }

        public long CountOfIndexes { get; set; }

        public string[] StaleIndexes { get; set; }

        public long CountOfStaleIndexes { get; set; }

        public long CountOfIndexingErrors { get; set; }

        public void CombineWith(FooterStatistics stats)
        {
            CountOfDocuments += stats.CountOfDocuments;

            CountOfIndexes = stats.CountOfIndexes; // every node has the same amount of indexes
            CountOfIndexingErrors += stats.CountOfIndexingErrors;

            if (StaleIndexes == null)
                StaleIndexes = stats.StaleIndexes;
            else if (stats.StaleIndexes != null)
            {
                var staleIndexes = new HashSet<string>(StaleIndexes, StringComparer.OrdinalIgnoreCase);
                staleIndexes.UnionWith(stats.StaleIndexes);

                StaleIndexes = staleIndexes.ToArray();
;            }

            CountOfStaleIndexes = StaleIndexes?.Length ?? 0;
        }
    }
}
