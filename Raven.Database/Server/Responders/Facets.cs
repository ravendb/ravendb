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

			var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);
			var facetStart = context.GetFacetStartFromHttpContext();
			var facetPageSize = context.GetFacetPageSizeFromHttpContext();

		    List<Facet> facets;

		    Etag etag;

            if (TryGetFacets(context, index, out etag, out facets) == false) 
				return;

			if (context.MatchEtag(etag))
			{
				context.SetStatusToNotModified();
				return;
			}

            context.WriteETag(etag);

            context.WriteJson(Database.ExecuteGetTermsQuery(index, indexQuery, facets, facetStart, facetPageSize));
		}

		private bool TryGetFacets(IHttpContext context, string index, out Etag etag, out List<Facet> facets)
		{
			etag = null;
			facets = null;
			switch (context.Request.HttpMethod)
			{
				case "GET":
					var facetSetupDoc = context.GetFacetSetupDocFromHttpContext();
					if (string.IsNullOrEmpty(facetSetupDoc))
					{
						var facetsJson = context.Request.QueryString["facets"];
						if (string.IsNullOrEmpty(facetsJson) == false)
							return TryGetFacetsFromString(context, index, out etag, out facets, facetsJson);
					}

					JsonDocument jsonDocument = Database.Get(facetSetupDoc, null);
					if (jsonDocument == null)
					{
						context.SetStatusToNotFound();
						context.Write("Could not find facet document: " + facetSetupDoc);
						return false;
					}

					etag = GetFacetsEtag(jsonDocument, index);

					facets = jsonDocument.DataAsJson.JsonDeserialization<FacetSetup>().Facets;

					if (facets == null || !facets.Any())
					{
						context.SetStatusToNotFound();
						context.Write("No facets found in facets setup document:" + facetSetupDoc);
						return false;
					}
					break;
				case "POST":
					return TryGetFacetsFromString(context, index, out etag, out facets, context.ReadString());
				default:
					context.SetStatusToBadRequest();
					context.Write("No idea how to handle this request");
					break;
			}
			return true;
		}

		private bool TryGetFacetsFromString(IHttpContext context, string index, out Etag etag, out List<Facet> facets,
		                                    string facetsJson)
		{
			etag = GetFacetsEtag(facetsJson, index);

			facets = JsonConvert.DeserializeObject<List<Facet>>(facetsJson);

			if (facets == null || !facets.Any())
			{
				context.SetStatusToBadRequest();
				context.Write("No facets found in request body");
				return false;
			}
			return true;
		}

		private Etag GetFacetsEtag(JsonDocument jsonDocument, string index)
		{
			return jsonDocument.Etag.HashWith(Database.GetIndexEtag(index, null));
		}

		private Etag GetFacetsEtag(string jsonFacets, string index)
        {
            using (var md5 = MD5.Create())
            {
                var etagBytes = md5.ComputeHash(Database.GetIndexEtag(index, null).ToByteArray().Concat(Encoding.UTF8.GetBytes(jsonFacets)).ToArray());
	            return Etag.Parse(etagBytes);
            }
        }
	}
}
