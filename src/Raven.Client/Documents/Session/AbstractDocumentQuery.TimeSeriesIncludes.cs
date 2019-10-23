using System.Collections.Generic;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        internal List<TimeSeriesIncludesToken> TimeSeriesIncludesTokens;

        protected void IncludeTimeSeries(string alias, Dictionary<string, HashSet<TimeSeriesRange>> timeSeriesToInclude)
        {
            if (timeSeriesToInclude?.Count > 0 == false)
                return;

            TimeSeriesIncludesTokens = new List<TimeSeriesIncludesToken>();
            _includesAlias = _includesAlias ?? alias;

            foreach (var kvp in timeSeriesToInclude)
            {
                foreach (var range in kvp.Value)
                {
                    TimeSeriesIncludesTokens.Add(TimeSeriesIncludesToken.Create(kvp.Key, range));
                }
            }
        }
    }
}
