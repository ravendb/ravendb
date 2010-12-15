using System;
using Raven.Abstractions.Data;
using Raven.Database.Queries;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
    public class SuggestionResponder : RequestResponder
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
            
            var term = context.Request.QueryString["term"];
            var field = context.Request.QueryString["field"];

            StringDistanceTypes distanceTypes;
            int numOfSuggestions;
            float accuracy;

            if (Enum.TryParse(context.Request.QueryString["distance"], true, out distanceTypes) == false)
                distanceTypes =StringDistanceTypes.Default;

            if (int.TryParse(context.Request.QueryString["max"], out numOfSuggestions) == false)
                numOfSuggestions = 10;

            if(float.TryParse(context.Request.QueryString["accuracy"], out accuracy) == false)
                accuracy = 0.5f;

            var query = new SuggestionQuery
                            {
                                Distance = distanceTypes,
                                Field = field,
                                MaxSuggestions = numOfSuggestions,
                                Term = term,
                                Accuracy = accuracy
                            };

            var suggestionQueryResult = Database.ExecuteSuggestionQuery(index, query);
            context.WriteJson(suggestionQueryResult);
        }
    }
}