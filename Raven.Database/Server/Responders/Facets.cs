using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Queries;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Responders
{
	public class Facets : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/facets/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET", "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[1].Value;

			var facetSetupDoc = context.GetFacetSetupDocFromHttpContext();
			var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);
			var facetStart = context.GetFacetStartFromHttpContext();
			var facetPageSize = context.GetFacetPageSizeFromHttpContext();

		    List<Facet> facets;

		    Guid etag;

            if (context.Request.HttpMethod == "GET")
            {
                JsonDocument jsonDocument = Database.Get(facetSetupDoc, null);

                if (jsonDocument == null)
                {
                    context.SetStatusToNotFound();
                    context.Write("Could not find facet document: " + facetSetupDoc);
                    return;
                }

                etag = GetFacetsEtag(jsonDocument, index);

                if (context.MatchEtag(etag))
                {
                    context.SetStatusToNotModified();
                    return;
                }

                facets = jsonDocument.DataAsJson.JsonDeserialization<FacetSetup>().Facets;

                if (facets == null || !facets.Any())
                {
                    context.SetStatusToNotFound();
                    context.Write("No facets found in facets setup document:" + facetSetupDoc);
                    return;
                }
            }
            else
            {
                var facetsJson = context.ReadString();
                
                etag = GetFacetsEtag(facetsJson, index);

                if (context.MatchEtag(etag))
                {
                    context.SetStatusToNotModified();
                    return;
                }

                facets = JsonConvert.DeserializeObject<List<Facet>>(facetsJson);

                if (facets == null || !facets.Any())
                {
                    context.SetStatusToNotFound();
                    context.Write("No facets found in request body");
                    return;
                }
            }

            context.WriteETag(etag);

            context.WriteJson(Database.ExecuteGetTermsQuery(index, indexQuery, facets, facetStart, facetPageSize));
		}

		private Guid GetFacetsEtag(JsonDocument jsonDocument, string index)
		{
			Guid etag;
			using (var md5 = MD5.Create())
			{
				var etagBytes = md5.ComputeHash(Database.GetIndexEtag(index, null).ToByteArray().Concat(jsonDocument.Etag.Value.ToByteArray()).ToArray());
				etag = new Guid(etagBytes);
			}
			return etag;
		}

        private Guid GetFacetsEtag(string jsonFacets, string index)
        {
            Guid etag;
            using (var md5 = MD5.Create())
            {
                var etagBytes = md5.ComputeHash(Database.GetIndexEtag(index, null).ToByteArray().Concat(Encoding.UTF8.GetBytes(jsonFacets)).ToArray());
                etag = new Guid(etagBytes);
            }
            return etag;
        }
	}
}