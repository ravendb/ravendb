using System.Net;

namespace Raven.Server.Responders
{
    public class RavenRoot : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/raven$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET"}; }
        }

        public override void Respond(HttpListenerContext context)
        {
            context.Response.Redirect("/raven/index.html");
        }
    }
}