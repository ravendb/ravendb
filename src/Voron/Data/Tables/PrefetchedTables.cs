using System;
using System.Collections.Generic;
using Sparrow;

namespace Voron.Data.Tables
{
    public class PrefetchedTables
    {
        public HashSet<string> AlreadyAccessed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public HashSet<ulong> AccessedGlobalIndexes = new HashSet<ulong>(NumericEqualityComparer.Instance);
    }
}