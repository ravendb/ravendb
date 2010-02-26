using System;
using System.Text.RegularExpressions;
using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class GetDocument : KayakResponder
    {
        public override string UrlPattern
        {
            get { return @"/docs/([\w\d_-]+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET"}; }
        }

        protected override void Respond(KayakContext context)
        {
            var match = urlMatcher.Match(context.Request.RequestUri);
            var docId = match.Groups[1].Value;
            var doc = Database.Get(docId);
            if (doc != null)
                context.Response.Write(doc);
            else
                context.Response.SetStatusToNotFound();
        }
    }
}