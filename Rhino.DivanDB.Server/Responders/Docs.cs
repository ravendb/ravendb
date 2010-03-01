using System.Net;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Server.Responders
{
    public class Docs : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/docs/?$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET", "POST"}; }
        }

        public override void Respond(HttpListenerContext context)
        {
            switch (context.Request.HttpMethod)
            {
                case "GET":
                    context.WriteJson(Database.GetDocuments(context.GetStart(), context.GetPageSize()));
                    break;
                case "POST":
                    var json = context.ReadJson();
                    var idProp = json.Property("_id");
                    if (idProp != null) 
                    {
                        context.SetStatusToBadRequest();
                        context.Write("POST to " + context.Request.Url.LocalPath +" with a document conatining '_id'");
                        return;
                    }
                    var id = Database.Put(json, new JObject());

                    context.SetStatusToCreated("/docs/" + id);
                    context.WriteJson(new { id });
                    break;
            }
        }
    }
}