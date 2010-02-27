using System;
using System.Text.RegularExpressions;
using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Document : KayakResponder
    {
        public override string UrlPattern
        {
            get { return @"/docs/([\w\d_-]+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET","DELETE"}; }
        }

        protected override void Respond(KayakContext context)
        {
            var match = urlMatcher.Match(context.Request.RequestUri);
            var docId = match.Groups[1].Value;
            switch (context.Request.Verb)
            {
                case "GET":
                    context.WriteData(Database.Get(docId));
                    break;
                case "DELETE":
                    Database.Delete(docId);
                    context.Response.SetStatusToDeleted();
                    break;
            }
        }
    }
}