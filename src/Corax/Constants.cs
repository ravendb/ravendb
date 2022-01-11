using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax
{
    public static class Constants
    {
        public static class Boosting
        {
            public static float ScoreEpsilon = 1e-8F;
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
        
    }
}
