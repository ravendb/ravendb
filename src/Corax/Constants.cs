using System;

namespace Corax
{
    public static class Constants
    {
        public static class Boosting
        {
            public static float ScoreEpsilon = 1e-8F;
        }

        public static class IndexSearcher
        {
            public const int TakeAll = -1;
            public const int NonAnalyzer = -1;
        }

        public static class StorageMask
        {
            public const long ContainerType = ~0b11;
        }
        
        public static class Search
        {
            public const byte Wildcard = (byte)'*';
            [Flags]
            internal enum SearchMatchOptions
            {
                TermMatch = 0,
                StartsWith = 1,
                EndsWith = 2,
                Contains = 4
            }

            public enum Operator
            {
                Or,
                And
            }
        }

        public static class Suggestions
        {
            public const int DefaultNGramSize = 4;

            public const float JaroWinklerThreshold = 0.7f;
            
            public enum Algorithm
            {
                NGram,
                JaroWinkler,
                Levenshtein 
            }
        }
        
    }
}
