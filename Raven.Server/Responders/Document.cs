using System;
using System.Net;

namespace Raven.Server.Responders
{
    public class Document : RequestResponder
    {
        public override string UrlPattern
        {
            get { return @"/docs/([\w\d_-]+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET", "DELETE", "PUT" }; }
        }

        public override void Respond(HttpListenerContext context)
        {
            var match = urlMatcher.Match(context.Request.Url.LocalPath);
            var docId = match.Groups[1].Value;
            switch (context.Request.HttpMethod)
            {
                case "GET":
                    Get(context, docId);
                    break;
                case "DELETE":
                    Database.Delete(docId, context.GetEtag());
                    context.SetStatusToDeleted();
                    break;
                case "PUT":
                    Put(context, docId);
                    break;
            }
        }

        private void Get(HttpListenerContext context, string docId)
        {
            var doc = Database.Get(docId);

            if (doc == null)
            {
                context.SetStatusToNotFound();
                return;
            }

            new DocumentRenderer(doc, context, Database).Render();
        }

        private void Put(HttpListenerContext context, string docId)
        {
            var json = context.ReadJson();
            context.SetStatusToCreated("/docs/" + docId);
            var id = Database.Put(docId, context.GetEtag(), json, context.Request.Headers.FilterHeaders());
            context.WriteJson(new { id });
        }
    }
}