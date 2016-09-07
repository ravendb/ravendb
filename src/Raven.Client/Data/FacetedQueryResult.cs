using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client.Data
{
    public class FacetedQueryResult
    {
        /// <summary>
        /// A list of results for the facet.  One entry for each term/range as specified in the facet setup document.
        /// </summary>
        public Dictionary<string, FacetResult> Results { get; set; }

        /// <summary>
        /// Indicates how much time it took to process facets on server.
        /// </summary>
        public TimeSpan Duration { get; set; }

        public long? IndexStateEtag { get; set; }

        public FacetedQueryResult()
        {
            Results = new Dictionary<string, FacetResult>();
        }
    }
}
