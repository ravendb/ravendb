using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
    public class LinearQueryResponder : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/linearQuery$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"POST"}; }
        }

        public override void Respond(IHttpContext context)
        {
            var query = context.ReadJson().JsonDeserialization<LinearQuery>();
            var linearQueryResults = Database.ExecuteQueryUsingLinearSearch(query);
            context.WriteJson(linearQueryResults);
        }
    }
}