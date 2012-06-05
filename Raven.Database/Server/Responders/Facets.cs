using System;
using System.Security.Cryptography;
using Raven.Abstractions.Data;
using Raven.Database.Queries;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using System.Linq;

namespace Raven.Database.Server.Responders
{
	public class Facets : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/facets/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[1].Value;

			var facetSetupDoc = context.GetFacetSetupDocFromHttpContext();
			var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);

			var jsonDocument = Database.Get(facetSetupDoc, null);
			if(jsonDocument == null)
			{
				context.SetStatusToNotFound();
				context.Write("Could not find facet document: " + facetSetupDoc);
				return;
			}

			var etag = GetFacetsEtag(jsonDocument, index);

			if(context.MatchEtag(etag))
			{
				context.SetStatusToNotModified();
				return;
			}
			context.WriteETag(etag);
			context.WriteJson(Database.ExecuteGetTermsQuery(index, indexQuery, facetSetupDoc));
		}

		private Guid GetFacetsEtag(JsonDocument jsonDocument, string index)
		{
			Guid etag;
			using(var md5 = MD5.Create())
			{
				var etagBytes = md5.ComputeHash(Database.GetIndexEtag(index, null).ToByteArray().Concat(jsonDocument.Etag.Value.ToByteArray()).ToArray());
				etag = new Guid(etagBytes);
			}
			return etag;
		}
	}	
}