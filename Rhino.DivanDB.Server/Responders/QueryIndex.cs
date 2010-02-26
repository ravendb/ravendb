using System;
using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class QueryIndex : KayakResponder
    {
        public override string UrlPattern
        {
            get { return @"/queries/([\w\d_-]+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }

        protected override void Respond(KayakContext context)
        {
            var match = urlMatcher.Match(context.Request.RequestUri);
            var index = match.Groups[1].Value;
            var query = context.Request.QueryString["query"];

            if (query == null)
            {
                context.WriteJson(new { view = Database.ViewStorage.GetViewDefinition(index) });
            }
            else
            {
                context.WriteJson(Database.Query(index, query));
            }
        }
    }
}