//-----------------------------------------------------------------------
// <copyright file="SuggestionResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Queries;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class SuggestionResponder : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			//suggest/index?term={0}&field={1}&numOfSuggestions={2}&distance={3}&accuracy={4}
			get { return "/suggest/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		/// <summary>
		/// Responds the specified context.
		/// </summary>
		/// <param name="context">The context.</param>
		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[1].Value;

			var indexEtag = Database.GetIndexEtag(index, null);
			if (context.MatchEtag(indexEtag))
			{
				context.SetStatusToNotModified();
				return;
			}

			var term = context.Request.QueryString["term"];
			var field = context.Request.QueryString["field"];

			StringDistanceTypes distanceTypes;
			int numOfSuggestions;
			float accuracy;

			if (Enum.TryParse(context.Request.QueryString["distance"], true, out distanceTypes) == false)
				distanceTypes = StringDistanceTypes.Default;

			if (int.TryParse(context.Request.QueryString["max"], out numOfSuggestions) == false)
				numOfSuggestions = 10;

			if (float.TryParse(context.Request.QueryString["accuracy"], out accuracy) == false)
				accuracy = 0.5f;

			bool popularity;
			if (bool.TryParse(context.Request.QueryString["popularity"], out popularity) == false)
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
			context.WriteETag(Database.GetIndexEtag(index, null));
			context.WriteJson(suggestionQueryResult);
		}
	}
}