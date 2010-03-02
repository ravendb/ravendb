using System.Collections.Specialized;
using System.Net;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Server.Responders
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
                    context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
<<<<<<< HEAD
                    var bytes = Database.Get(docId);
                    if(bytes==null)
=======
                    var doc = Database.Get(docId);
                    if(doc==null)
>>>>>>> luke
                    {
                        context.SetStatusToNotFound();
                        return;
                    }
<<<<<<< HEAD
                    context.WriteData(bytes, new NameValueCollection());
=======
                    context.WriteData(doc.Data, doc.Metadata);
>>>>>>> luke
                    break;
                case "DELETE":
                    Database.Delete(docId);
                    context.SetStatusToDeleted();
                    break;
                case "PUT":
                    Put(context, docId);
                    break;
            }
        }

        private void Put(HttpListenerContext context, string docId)
        {
            var json = context.ReadJson();
<<<<<<< HEAD
            var idProp = json.Property("_id");
            if (idProp == null) // set the in-document id based on the url
            {
                json.Add("_id", new JValue(docId));
            }
            else
            {
                var idVal = idProp.Value.Value<object>();
                if (idVal != null && idVal.ToString() != docId) // doc id conflict
                {
                    context.SetStatusToBadRequest();
                    var err = string.Format(
                        "PUT on {0} but the document contained '_id' property with: '{1}'",
                        context.Request.Url.LocalPath, idVal);
                    context.Write(err);
                    return;
                }
            }
            context.SetStatusToCreated("/docs/" + docId);
            context.WriteJson(new { id = Database.Put(json) });
=======
            context.SetStatusToCreated("/docs/" + docId);
            context.WriteJson(new { id = Database.Put(docId, json, 
                context.Request.Headers.FilterHeaders()
                ) });
>>>>>>> luke
        }
    }
}