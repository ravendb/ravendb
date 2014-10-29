//-----------------------------------------------------------------------
// <copyright file="FacetedQueryRunnerExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Queries
{
	public static class FacetedQueryRunnerExtensions
	{
        public static FacetResults ExecuteGetTermsQuery(this DocumentDatabase self,
          string index, IndexQuery query, string facetSetupDoc)
        {
            return self.ExecuteGetTermsQuery(index, query, facetSetupDoc, 0, null);
        }

        public static FacetResults ExecuteGetTermsQuery(this DocumentDatabase self, string index, IndexQuery query, string facetSetupDoc, int start, int? pageSize)
        {
            var facetSetup = self.Documents.Get(facetSetupDoc, null);
            if (facetSetup == null)
                throw new InvalidOperationException("Could not find facets document: " + facetSetupDoc);

            var facets = facetSetup.DataAsJson.JsonDeserialization<FacetSetup>().Facets;

            return self.ExecuteGetTermsQuery(index, query, facets, start, pageSize);
        }

        public static FacetResults ExecuteGetTermsQuery(this DocumentDatabase self, string index, IndexQuery query, List<Facet> facets)
        {
            return self.ExecuteGetTermsQuery(index, query, facets, 0, null);
        }

        public static FacetResults ExecuteGetTermsQuery(this DocumentDatabase self, string index, IndexQuery query, List<Facet> facets, int start, int? pageSize)
        {
            return new FacetedQueryRunner(self).GetFacets(index, query, facets, start, pageSize);
        }
	}
}
