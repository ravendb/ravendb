using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Database.Queries;

namespace Raven.Database.Server.Controllers
{
    public class FacetsController : RavenDbApiController
    {
        [HttpGet]
        [Route("facets/{*id}")]
        [Route("databases/{databaseName}/facets/{*id}")]
        public async Task<HttpResponseMessage> FacetsGet(string id)
        {
            List<Facet> facets = null;
            byte[] additionalEtagBytes = null;
           
            var facetSetupDoc = GetQueryStringValue("facetDoc") ;
            if (string.IsNullOrEmpty(facetSetupDoc))
            {
                var facetsJson = GetQueryStringValue("facets");
                if (string.IsNullOrEmpty(facetsJson) == false)
                {
					additionalEtagBytes = Encoding.UTF8.GetBytes(facetsJson);

					var msg = TryGetFacetsFromString(facetsJson, out facets);
					if (msg != null)
						return msg;
                }
            }
            else
            {
                var jsonDocument = Database.Documents.Get(facetSetupDoc, null);
	            if (jsonDocument == null)
		            return GetMessageWithString("Could not find facet document: " + facetSetupDoc, HttpStatusCode.NotFound);

	            additionalEtagBytes = jsonDocument.Etag.ToByteArray();
                facets = jsonDocument.DataAsJson.JsonDeserialization<FacetSetup>().Facets;
            }

			var etag = GetFacetsEtag(id, additionalEtagBytes);
            if (MatchEtag(etag))
            {
                return GetEmptyMessage(HttpStatusCode.NotModified);
            }

            if (facets == null || !facets.Any())
                return GetMessageWithString("No facets found in facets setup document:" + facetSetupDoc, HttpStatusCode.NotFound);

            return await ExecuteFacetsQuery(id, facets, etag);
        }

        [HttpPost]
        [Route("facets/{*id}")]
        [Route("databases/{databaseName}/facets/{*id}")]
        public async Task<HttpResponseMessage> FacetsPost(string id)
        {
            List<Facet> facets;
            var facetsJson = await ReadStringAsync();
            var msg = TryGetFacetsFromString(facetsJson, out facets);
            if (msg != null)
                return msg;

	        var etag = GetFacetsEtag(id, Encoding.UTF8.GetBytes(facetsJson));
			if (MatchEtag(etag))
			{
				return GetEmptyMessage(HttpStatusCode.NotModified);
			}

            return await ExecuteFacetsQuery(id, facets, etag);
        }

        [HttpPost]
        [Route("facets/multisearch")]
        [Route("databases/{databaseName}/facets/multisearch")]
        public async Task<HttpResponseMessage> MultiSearch()
        {
            var str = await ReadStringAsync();
            var facetedQueries = JsonConvert.DeserializeObject<FacetQuery[]>(str);

            var results =
                facetedQueries.Select(
                    facetedQuery =>
                    {
                        if (facetedQuery.FacetSetupDoc != null)
                            return Database.ExecuteGetTermsQuery(facetedQuery.IndexName, facetedQuery.Query, facetedQuery.FacetSetupDoc,
                                facetedQuery.PageStart, facetedQuery.PageSize);
                        if (facetedQuery.Facets != null)
                            return Database.ExecuteGetTermsQuery(facetedQuery.IndexName, facetedQuery.Query, facetedQuery.Facets,
                                facetedQuery.PageStart,
                                facetedQuery.PageSize);

                        throw new InvalidOperationException("Missing a facet setup document or a list of facets");
                    }).ToArray();

            return GetMessageWithObject(results);
        }

		private async Task<HttpResponseMessage> ExecuteFacetsQuery(string index, List<Facet> facets, Etag indexEtag)
        {
            var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
            var facetStart = GetFacetStart();
            var facetPageSize = GetFacetPageSize();
            var results = Database.ExecuteGetTermsQuery(index, indexQuery, facets, facetStart, facetPageSize);
            return GetMessageWithObject(results, HttpStatusCode.OK, indexEtag);
        }

        private HttpResponseMessage TryGetFacetsFromString(string facetsJson, out List<Facet> facets)
        {
            facets = JsonConvert.DeserializeObject<List<Facet>>(facetsJson);

            if (facets == null || !facets.Any())
                return GetMessageWithString("No facets found in request", HttpStatusCode.BadRequest);

            return null;
        }


        private Etag GetFacetsEtag(string index, byte[] additionalEtagBytes)
        {
            using (var md5 = MD5.Create())
            {
                var etagBytes = md5.ComputeHash(Database.Indexes.GetIndexEtag(index, null).ToByteArray().Concat(additionalEtagBytes).ToArray());
                return Etag.Parse(etagBytes);
            }
        }

        private int GetFacetStart()
        {
            int start;
            return int.TryParse(GetQueryStringValue("facetStart"), out start) ? start : 0;
        }

        private int? GetFacetPageSize()
        {
            int pageSize;
            if (int.TryParse(GetQueryStringValue("facetPageSize"), out pageSize))
                return pageSize;
            return null;
        }
    }
}