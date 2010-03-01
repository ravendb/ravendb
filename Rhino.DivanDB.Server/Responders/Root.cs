using System.Net;

namespace Rhino.DivanDB.Server.Responders
{
    public class Root : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET"}; }
        }

        public override void Respond(HttpListenerContext context)
        {
            context.Response.Redirect("/divan/index.html");
        }
    }
}