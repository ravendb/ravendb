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
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Data
{
    public class FacetQuery
    {
        private IReadOnlyList<Facet> _facets;
        private string _facetsAsJson;
        private int _pageSize = IndexQuery.DefaultPageSize;
        private bool _pageSizeSet;

        public FacetQuery()
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
                _facetsAsJson = null;
            }
        }

        public bool IsDistinct { get; set; }

        public QueryOperator DefaultOperator { get; set; }

        public string DefaultField { get; set; }

        public string Query { get; set; }

        public int Start { get; set; }

        public int PageSize
        {
            get { return _pageSize; }
            set
            {
                _pageSize = value;
                _pageSizeSet = true;
            }
        }

        public string[] FieldsToFetch { get; set; }

        public HttpMethod CalculateHttpMethod()
        {
            if (Facets == null || Facets.Count == 0)
                return HttpMethod.Get;

            if (_facetsAsJson == null)
                _facetsAsJson = SerializeFacetsToFacetsJsonString(Facets);

            return _facetsAsJson.Length < 32 * 1024 - 1 ? HttpMethod.Get : HttpMethod.Post;
        }

        public string GetFacetsAsJson()
        {
            return _facetsAsJson ?? (_facetsAsJson = SerializeFacetsToFacetsJsonString(Facets));
        }

        public string GetQueryString(HttpMethod method)
        {
            var path = new StringBuilder();

            if (Start != 0)
                path.Append("&start=").Append(Start);

            if (_pageSizeSet)
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

            if (string.IsNullOrWhiteSpace(FacetSetupDoc) == false)
                path.Append("&facetDoc=").Append(FacetSetupDoc);

            if (method == HttpMethod.Get && Facets != null && Facets.Count > 0)
                path.Append("&facets=").Append(GetFacetsAsJson());

            path.Append("&op=facets");

            return path.ToString();
        }

        public static FacetQuery Parse(IQueryCollection query, int start, int pageSize)
        {
            var result = new FacetQuery
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

            return result;
        }

        private static string SerializeFacetsToFacetsJsonString(IReadOnlyList<Facet> facets)
        {
            var ravenJArray = (RavenJArray)RavenJToken.FromObject(facets, new JsonSerializer
            {
                NullValueHandling = NullValueHandling.Ignore,
                DefaultValueHandling = DefaultValueHandling.Ignore,
            });
            foreach (var facet in ravenJArray)
            {
                var obj = (RavenJObject)facet;
                if (obj.Value<string>("Name") == obj.Value<string>("DisplayName"))
                    obj.Remove("DisplayName");
                var jArray = obj.Value<RavenJArray>("Ranges");
                if (jArray != null && jArray.Length == 0)
                    obj.Remove("Ranges");
            }
            return ravenJArray.ToString(Formatting.None);
        }
    }
}
