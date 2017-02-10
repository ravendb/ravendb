// -----------------------------------------------------------------------
//  <copyright file="FacetQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
#if !NET46
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
#endif
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Document;
using Sparrow.Json.Parsing;


namespace Raven.NewClient.Client.Data
{
    public class FacetQuery : IndexQueryBase
    {
        private IReadOnlyList<Facet> _facets;
        private DynamicJsonArray _facetsAsDynamicJsonArray;

        public FacetQuery(DocumentConvention conventions) : base(conventions)
        {
        }

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
        public IReadOnlyList<Facet> Facets
        {
            get { return _facets; }
            set
            {
                _facets = value;
                _facetsAsDynamicJsonArray = null;
            }
        }

        public HttpMethod CalculateHttpMethod()
        {
            if (Facets == null || Facets.Count == 0)
                return HttpMethod.Get;

            if (_facetsAsDynamicJsonArray == null)
                _facetsAsDynamicJsonArray = SerializeFacetsToDynamicJsonArray(Facets);

            return HttpMethod.Post;
        }

        public DynamicJsonArray GetFacetsAsJson()
        {
            return _facetsAsDynamicJsonArray ?? (_facetsAsDynamicJsonArray = SerializeFacetsToDynamicJsonArray(Facets));
        }

        public string GetQueryString(HttpMethod method)
        {
            var path = new StringBuilder();

            if (Start != 0)
                path.Append("&start=").Append(Start);

            path.Append("&pageSize=").Append(PageSize);

            if (string.IsNullOrEmpty(Query) == false)
                path.Append("&query=").Append(EscapingHelper.EscapeLongDataString(Query));

            if (string.IsNullOrEmpty(DefaultField) == false)
                path.Append("&defaultField=").Append(Uri.EscapeDataString(DefaultField));

            if (DefaultOperator != QueryOperator.Or)
                path.Append("&operator=AND");

            if (IsDistinct)
                path.Append("&distinct=true");

            FieldsToFetch.ApplyIfNotNull(field => path.Append("&fetch=").Append(Uri.EscapeDataString(field)));

            if (CutoffEtag != null)
                path.Append("&cutOffEtag=").Append(CutoffEtag);

            if (WaitForNonStaleResultsAsOfNow)
                path.Append("&waitForNonStaleResultsAsOfNow=true");

            if (WaitForNonStaleResultsTimeout != null)
                path.AppendLine("&waitForNonStaleResultsTimeout=" + WaitForNonStaleResultsTimeout);

            if (string.IsNullOrWhiteSpace(FacetSetupDoc) == false)
                path.Append("&facetDoc=").Append(FacetSetupDoc);

            if (method == HttpMethod.Get && Facets != null && Facets.Count > 0)
                path.Append("&facets=").Append(GetFacetsAsJson());

            path.Append("&op=facets");

            return path.ToString();
        }

#if !NET46
        public static FacetQuery Parse(IQueryCollection query, int start, int pageSize, DocumentConvention conventions)
        {
            var result = new FacetQuery(conventions)
            {
                Start = start,
                PageSize = pageSize
            };

            StringValues values;
            if (query.TryGetValue("facetDoc", out values))
                result.FacetSetupDoc = values.First();

            if (query.TryGetValue("distinct", out values))
                result.IsDistinct = bool.Parse(values.First());

            if (query.TryGetValue("operator", out values))
                result.DefaultOperator = "And".Equals(values.First(), StringComparison.OrdinalIgnoreCase) ? QueryOperator.And : QueryOperator.Or;

            if (query.TryGetValue("defaultField", out values))
                result.DefaultField = values.First();

            if (query.TryGetValue("query", out values))
                result.Query = values.First();

            if (query.TryGetValue("fetch", out values))
                result.FieldsToFetch = values.ToArray();

            if (query.TryGetValue("cutOffEtag", out values))
                result.CutoffEtag = long.Parse(values.First());

            if (query.TryGetValue("waitForNonStaleResultsAsOfNow", out values))
                result.WaitForNonStaleResultsAsOfNow = bool.Parse(values.First());

            if (query.TryGetValue("waitForNonStaleResultsTimeout", out values))
                result.WaitForNonStaleResultsTimeout = TimeSpan.Parse(values.First());

            return result;
        }
#endif

        public static FacetQuery Create(string indexName, IndexQueryBase query, string facetSetupDoc, List<Facet> facets, int start, int? pageSize, DocumentConvention conventions)
        {
            var result = new FacetQuery(conventions)
            {
                IndexName = indexName,
                CutoffEtag = query.CutoffEtag,
                DefaultField = query.DefaultField,
                DefaultOperator = query.DefaultOperator,
                FieldsToFetch = query.FieldsToFetch,
                IsDistinct = query.IsDistinct,
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

        private static DynamicJsonArray SerializeFacetsToDynamicJsonArray(IEnumerable<Facet> facets)
        {
            var array = new DynamicJsonArray();

            foreach (var facet in facets)
                array.Add(facet.ToJson());

            return array;
        }
    }
}
