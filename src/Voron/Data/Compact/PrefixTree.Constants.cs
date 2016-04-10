using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    partial class PrefixTree
    {
        public unsafe static class Constants
        {
            public const long TombstoneNodeName = unchecked((long)0xFFFFFFFFFFFFFFF0);
            public const long HeadNodeName = unchecked((long)0xFFFFFFFFFFFFFFF1);
            public const long TailNodeName = unchecked((long)0xFFFFFFFFFFFFFFF2);
            public const long InvalidNodeName = -1;

            public const long InvalidPage = -1;

            public const int L1CacheSize = 16 * 1024;
            public static int NodesPerCacheLine = L1CacheSize / sizeof(PrefixTree.Node);
            public static int DepthPerCacheLine = (int) Math.Log(NodesPerCacheLine, 2);

            public static int TranslationTableInitialItems = 10000;

        }
    }
}
