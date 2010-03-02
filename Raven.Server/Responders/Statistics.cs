using System.Net;

namespace Raven.Server.Responders
{
    public class Statistics : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/stats"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET"}; }
        }
        public override void Respond(HttpListenerContext context)
        {
            context.WriteJson(Database.Statistics);
        }
    }
}