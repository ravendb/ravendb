using System.Net;

namespace Rhino.DivanDB.Server.Responders
{
    public class DivanRootResponser : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/divan$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }

        public override void Respond(HttpListenerContext context)
        {
            context.Response.Redirect("/divan/index.html");
        }
    }
}