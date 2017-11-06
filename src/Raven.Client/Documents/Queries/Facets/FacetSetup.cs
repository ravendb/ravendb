using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Facets
{
    public class FacetSetup
    {
        /// <summary>
        /// Id of a facet setup document.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// List of facets.
        /// </summary>
        public List<Facet> Facets { get; set; }

        /// <summary>
        /// List of range facets.
        /// </summary>
        public List<RangeFacet> RangeFacets { get; set; }

        public FacetSetup()
        {
            Facets = new List<Facet>();
            RangeFacets = new List<RangeFacet>();
        }
    }
}
