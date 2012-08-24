//-----------------------------------------------------------------------
// <copyright file="FacetedQueryRunnerExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Queries
{
	public static class FacetedQueryRunnerExtensions
	{
		public static FacetResults ExecuteGetTermsQuery(this DocumentDatabase self,
			string index, IndexQuery query, string facetSetupDoc)
		{
			return new FacetedQueryRunner(self).GetFacets(index, query, facetSetupDoc);
		}
	}
}
