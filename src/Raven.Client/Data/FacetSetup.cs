using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client.Data
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

        public FacetSetup()
        {
            Facets = new List<Facet>();
        }
    }
}
