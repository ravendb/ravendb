using System;
using Kayak;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Server.Responders
{
    public class Docs : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/docs/?$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET", "POST"}; }
        }

        protected override void Respond(KayakContext context)
        {
            switch (context.Request.Verb)
            {
                case "GET":
                    context.WriteJson(Database.GetDocuments(context.GetStart(), context.GetPageSize()));
                    break;
                case "POST":
                    var json = context.ReadJson();
                    var idProp = json.Property("_id");
                    if (idProp != null) 
                    {
                        context.Response.SetStatusToBadRequest();
                        context.Response.WriteLine("POST to " + context.Request.Path +" with a document conatining '_id'");
                        return;
                    }
                    var id = Database.Put(json);

                    context.SetStatusToCreated("/docs/" + id);
                    context.WriteJson(new { id });
                    break;
            }
        }
    }
}