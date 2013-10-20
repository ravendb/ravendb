using System;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Queries;

namespace Raven.Database.Server.Controllers
{
	public class SuggestionController : RavenApiController
	{
		[HttpGet]
		[Route("suggest/{*id}")]
		[Route("databases/{databaseName}/suggest/{*id}")]
		public HttpResponseMessage SuggestGet(string id)
		{
			var index = id;

			var indexEtag = Database.GetIndexEtag(index, null);
			if (MatchEtag(indexEtag))
				return GetEmptyMessage(HttpStatusCode.NotModified);

			var term = GetQueryStringValue("term");
			var field = GetQueryStringValue("field");

			StringDistanceTypes distanceTypes;
			int numOfSuggestions;
			float accuracy;

			if (Enum.TryParse(GetQueryStringValue("distance"), true, out distanceTypes) == false)
				distanceTypes = StringDistanceTypes.Default;

			if (distanceTypes == StringDistanceTypes.None)
			{
				return GetMessageWithObject(new
				{
					Error = "Suggestion is disabled since you specified the Distance value as 'StringDistanceTypes.None'."
				}, HttpStatusCode.BadRequest);
			}

			if (int.TryParse(GetQueryStringValue("max"), out numOfSuggestions) == false)
				numOfSuggestions = 10;

			if (float.TryParse(GetQueryStringValue("accuracy"), out accuracy) == false)
				accuracy = 0.5f;

			bool popularity;
			if (bool.TryParse(GetQueryStringValue("popularity"), out popularity) == false)
				popularity = false;

			var query = new SuggestionQuery
			{
				Distance = distanceTypes,
				Field = field,
				MaxSuggestions = numOfSuggestions,
				Term = term,
				Accuracy = accuracy,
				Popularity = popularity
			};

			var suggestionQueryResult = Database.ExecuteSuggestionQuery(index, query);

			var msg = GetMessageWithObject(suggestionQueryResult);
			WriteETag(Database.GetIndexEtag(index, null), msg);
			return msg;
		}
	}
}