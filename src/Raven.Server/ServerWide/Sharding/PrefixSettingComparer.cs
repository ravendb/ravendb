using System;
using System.Collections.Generic;
using Raven.Client.ServerWide.Sharding;

namespace Raven.Server.ServerWide.Sharding
{
    public class PrefixedSettingComparer : IComparer<PrefixedShardingSetting>
    {
        private PrefixedSettingComparer()
        {
        }

        public static readonly PrefixedSettingComparer Instance = new();

        public int Compare(PrefixedShardingSetting x, PrefixedShardingSetting y)
        {
            // compare prefixes in descending order 
            return string.Compare(y?.Prefix, x?.Prefix, StringComparison.OrdinalIgnoreCase);
        }
    }

}
