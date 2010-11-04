using System;
using System.Globalization;

namespace Raven.Database.Indexing.Collation
{
    [CLSCompliant(false)]
    public class AbstractCultureCollationAnalyzer : CollationAnalyzer
    {
        public AbstractCultureCollationAnalyzer()
        {
            var culture = GetType().Name.Replace("CollationAnalyzer","").ToLowerInvariant();
            Init(CultureInfo.GetCultureInfo(culture));
        }
    }
}