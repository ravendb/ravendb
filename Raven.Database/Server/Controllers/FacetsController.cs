using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Queries;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class FacetsController : ClusterAwareRavenDbApiController
    {
        [HttpGet]
        [RavenRoute("facets/{*id}")]
        [RavenRoute("databases/{databaseName}/facets/{*id}")]
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

            return await ExecuteFacetsQuery(id, facets, etag).ConfigureAwait(false);
        }

        [HttpPost]
        [RavenRoute("facets/{*id}")]
        [RavenRoute("databases/{databaseName}/facets/{*id}")]
        public async Task<HttpResponseMessage> FacetsPost(string id)
        {
            if (id == "multisearch")
            {
                return await MultiSearch().ConfigureAwait(false);
            }

            List<Facet> facets;
            var facetsJson = await ReadStringAsync().ConfigureAwait(false);
            var msg = TryGetFacetsFromString(facetsJson, out facets);
            if (msg != null)
                return msg;

            var etag = GetFacetsEtag(id, Encoding.UTF8.GetBytes(facetsJson));
            if (MatchEtag(etag))
            {
                return GetEmptyMessage(HttpStatusCode.NotModified);
            }

            return await ExecuteFacetsQuery(id, facets, etag).ConfigureAwait(false);
        }

        [HttpPost]
        [RavenRoute("facets-multisearch")]
        [RavenRoute("databases/{databaseName}/facets-multisearch")]
        public async Task<HttpResponseMessage> MultiSearch()
        {
            var str = await ReadStringAsync().ConfigureAwait(false);
            
            var facetedQueries = JsonConvert.DeserializeObject<FacetQuery[]>(str);
            
            var results =
                facetedQueries.Select(
                    facetedQuery =>
                    {
                        FacetResults facetResults = null;

                        var curFacetEtag = GetFacetsEtag(facetedQuery.IndexName, Encoding.UTF8.GetBytes(facetedQuery.Query.Query + string.Concat(facetedQuery.Facets.Select(x => x.Name).ToArray())));

                        if (Database.IndexDefinitionStorage.Contains(facetedQuery.IndexName) == false)
                            throw new IndexDoesNotExistsException(string.Format("Index '{0}' does not exist.", facetedQuery.IndexName));

                        if (facetedQuery.FacetSetupDoc != null)
                            facetResults =  Database.ExecuteGetTermsQuery(facetedQuery.IndexName, facetedQuery.Query, facetedQuery.FacetSetupDoc,
                                facetedQuery.PageStart, facetedQuery.PageSize);
                        if (facetedQuery.Facets != null)
                            facetResults =  Database.ExecuteGetTermsQuery(facetedQuery.IndexName, facetedQuery.Query, facetedQuery.Facets,
                                facetedQuery.PageStart,
                                facetedQuery.PageSize);

                        if (facetResults != null)
                        {
                            facetResults.IndexStateEtag = curFacetEtag;
                            return facetResults;
                        }

                        throw new InvalidOperationException("Missing a facet setup document or a list of facets");
                    }).ToArray();

            return GetMessageWithObject(results, HttpStatusCode.OK);
        }

        private Task<HttpResponseMessage> ExecuteFacetsQuery(string index, List<Facet> facets, Etag indexEtag)
        {
            if (Database.IndexDefinitionStorage.Contains(index) == false)
                return GetMessageWithStringAsTask(string.Format("Index '{0}' does not exist.", index), HttpStatusCode.BadRequest);

            var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
            var facetStart = GetFacetStart();
            var facetPageSize = GetFacetPageSize();
            var results = Database.ExecuteGetTermsQuery(index, indexQuery, facets, facetStart, facetPageSize);
            var token = RavenJToken.FromObject(results, new JsonSerializer
            {
                NullValueHandling = NullValueHandling.Ignore,
                DefaultValueHandling = DefaultValueHandling.Ignore,
            });
            return GetMessageWithObjectAsTask(token, HttpStatusCode.OK, indexEtag);
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
            var etagBytes = Encryptor.Current.Hash.Compute16(Database.Indexes.GetIndexEtag(index, null).ToByteArray().Concat(additionalEtagBytes).ToArray());
            return Etag.Parse(etagBytes);
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
