// -----------------------------------------------------------------------
//  <copyright file="FacetQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class FacetQuery
	{
		public string IndexName { get; set; }

		public IndexQuery Query { get; set; }
		
		public string FacetSetupDoc { get; set; }
		
		public List<Facet> Facets { get; set; }
		
		public int PageStart { get; set; }
		
		public int? PageSize { get; set; }
	}
}