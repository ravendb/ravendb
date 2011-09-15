using Raven.Database.Queries;
using Raven.Http.Abstractions;
using Raven.Database.Extensions;
using Raven.Http.Extensions;

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

			//var facets = context.GetFacetsFromHttpContext();
			var facetSetupDoc = context.GetFacetSetupDocFromHttpContext();
			var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);

			context.WriteJson(Database.ExecuteGetTermsQuery(index, indexQuery, facetSetupDoc));
		}
	}	
}