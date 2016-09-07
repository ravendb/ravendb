// -----------------------------------------------------------------------
//  <copyright file="FacetQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client.Data
{
    public class FacetQuery
    {
        /// <summary>
        /// Index name to run facet query on.
        /// </summary>
        public string IndexName { get; set; }

        /// <summary>
        /// Query that should be ran on index.
        /// </summary>
        public IndexQuery Query { get; set; }

        /// <summary>
        /// Id of a facet setup document that can be found in database containing facets (mutually exclusive with Facets).
        /// </summary>
        public string FacetSetupDoc { get; set; }

        /// <summary>
        /// List of facets (mutually exclusive with FacetSetupDoc).
        /// </summary>
        public List<Facet> Facets { get; set; }

        /// <summary>
        /// Page start for facet query results.
        /// </summary>
        public int PageStart { get; set; }

        /// <summary>
        /// Page size for facet query results.
        /// </summary>
        public int? PageSize { get; set; }
    }
}
