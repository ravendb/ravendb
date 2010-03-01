using System.Linq;
using System.Net;
using System.Text.RegularExpressions;

namespace Rhino.DivanDB.Server.Responders
{
    public abstract class RequestResponder
    {
        public abstract string UrlPattern { get; }
        public abstract string[] SupportedVerbs { get; }

        protected readonly Regex urlMatcher;
        private readonly string[] supportedVerbsCached;

        public DocumentDatabase Database { get; set; }

        protected RequestResponder()
        {
            urlMatcher = new Regex(UrlPattern);
            supportedVerbsCached = SupportedVerbs;
        }

        public bool WillRespond(HttpListenerContext context)
        {
            var match = urlMatcher.Match(context.Request.Url.LocalPath);
            return match.Success && supportedVerbsCached.Contains(context.Request.HttpMethod);
        }


        public abstract void Respond(HttpListenerContext context);
    }
}