using System.Net;

namespace Raven.Server.Responders
{
    public class Favicon: RequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/favicon.ico$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }

        public override void Respond(HttpListenerContext context)
        {
            context.Response.Redirect("/divan/favicon.ico");
        }
    }
}