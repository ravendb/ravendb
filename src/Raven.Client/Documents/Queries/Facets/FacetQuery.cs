// -----------------------------------------------------------------------
//  <copyright file="FacetQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Queries.Facets
{
    public class FacetQuery : IndexQueryBase<Dictionary<string, object>>
    {
        public string[] FieldsToFetch { get; set; }

        public QueryOperator DefaultOperator { get; set; }

        public string DefaultField { get; set; }

        public bool IsDistinct { get; set; }

        /// <summary>
        /// Index name to run facet query on.
        /// </summary>
        public string IndexName { get; set; }

        /// <summary>
        /// Id of a facet setup document that can be found in database containing facets (mutually exclusive with Facets).
        /// </summary>
        public string FacetSetupDoc { get; set; }

        /// <summary>
        /// List of facets (mutually exclusive with FacetSetupDoc).
        /// </summary>
        public IReadOnlyList<Facet> Facets { get; set; }

#if !NET46
        public static FacetQuery Parse(IQueryCollection query, int start, int pageSize, DocumentConventions conventions)
        {
            var result = new FacetQuery
            {
                Start = start,
                PageSize = pageSize
            };

            StringValues values;
            if (query.TryGetValue("facetDoc", out values))
                result.FacetSetupDoc = values.First();

            if (query.TryGetValue("query", out values))
                result.Query = values.First();

            if (query.TryGetValue("cutOffEtag", out values))
                result.CutoffEtag = long.Parse(values.First());

            if (query.TryGetValue("waitForNonStaleResultsAsOfNow", out values))
                result.WaitForNonStaleResultsAsOfNow = bool.Parse(values.First());

            if (query.TryGetValue("waitForNonStaleResultsTimeout", out values))
                result.WaitForNonStaleResultsTimeout = TimeSpan.Parse(values.First());

            return result;
        }
#endif

        public static FacetQuery Create(string indexName, IndexQueryBase<Dictionary<string, object>> query, string facetSetupDoc, List<Facet> facets, int start, int? pageSize, DocumentConventions conventions)
        {
            var result = new FacetQuery
            {
                IndexName = indexName,
                CutoffEtag = query.CutoffEtag,
                Query = query.Query,
                WaitForNonStaleResults = query.WaitForNonStaleResults,
                WaitForNonStaleResultsAsOfNow = query.WaitForNonStaleResultsAsOfNow,
                WaitForNonStaleResultsTimeout = query.WaitForNonStaleResultsTimeout,
                Start = start,
                FacetSetupDoc = facetSetupDoc,
                Facets = facets
            };

            if (pageSize.HasValue)
                result.PageSize = pageSize.Value;

            return result;
        }
    }
}
