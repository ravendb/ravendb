using System;
using System.Linq;
using System.Text.RegularExpressions;
using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public abstract class KayakResponder : IKayakResponder
    {
        public abstract string UrlPattern { get; }
        public abstract string[] SupportedVerbs { get; }

        protected readonly Regex urlMatcher;
        private readonly string[] supportedVerbsCached;

        public DocumentDatabase Database { get; set; }

        protected KayakResponder()
        {
            urlMatcher = new Regex(UrlPattern);
            supportedVerbsCached = SupportedVerbs;
        }

        public void WillRespond(KayakContext context, Action<bool, Exception> callback)
        {
            try
            {
                var match = urlMatcher.Match(context.Request.Path);
                bool validRequest = match.Success && supportedVerbsCached.Contains(context.Request.Verb);
                callback(validRequest, null);
            }
            catch (Exception e)
            {
                callback(false, e);   
            }
        }

        public void Respond(KayakContext context, Action<Exception> callback)
        {
            try
            {
                Respond(context);
                callback(null);
            }
            catch (Exception e)
            {
                callback(e);
            }
        }

        protected abstract void Respond(KayakContext context);
    }
}