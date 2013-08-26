using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Debugging
{
    public class DebugQueries : AbstractRequestResponder
    {
        public override string UrlPattern
        {
            get { return @"^/debug/queries"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }

        public override void Respond(IHttpContext context)
        {
            context.WriteJson(Database.WorkContext.CurrentlyRunningQueries);            
        }
    }
}
