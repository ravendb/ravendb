using System;
using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class QueryIndex : KayakResponder
    {
        public override string UrlPattern
        {
            get { return @"/query/([\w\d_-]+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new []{"GET"}; }
        }

        protected override void Respond(KayakContext context)
        {
            var match = urlMatcher.Match(context.Request.RequestUri);
            var index = match.Groups[1].Value;
            var query = context.Request.QueryString["query"] ?? 
                "-stranger:things"; //match everything, basically

            context.WriteJson(Database.Query(index, query));
        }
    }
}