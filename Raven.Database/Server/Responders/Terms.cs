using System;
using Raven.Http.Abstractions;
using Raven.Database.Queries;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
    public class Terms : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/terms/(.+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new []{"GET"}; }
        }

        public override void Respond(IHttpContext context)
        {
            var match = urlMatcher.Match(context.GetRequestUrl());
            var index = match.Groups[1].Value;

            var field = context.Request.QueryString["field"];
            if(string.IsNullOrEmpty(field))
                field = null;

            context.WriteJson(Database.GetTerms(index, field));
        }
    }
}