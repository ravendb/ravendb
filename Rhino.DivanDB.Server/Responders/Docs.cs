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
                    context.WriteJson(Database.GetDocuments(GetStart(context), GetPageSize(context)));
                    break;
                case "POST":
                    context.Response.SetStatusToCreated();
                    var json = context.ReadJson();
                    var idProp = json.Property("_id");
                    if (idProp != null) 
                    {
                        context.Response.SetStatusToBadRequest();
                        context.Response.WriteLine("POST to " + context.Request.Path +" with a document conatining '_id'");
                        return;
                    }
                    context.WriteJson(new { id = Database.Put(json) });
                    break;
            }
        }

        private int GetStart(KayakContext context)
        {
            int start;
            int.TryParse(context.Request.QueryString["start"], out start);
            return start;
        }

        private int GetPageSize(KayakContext context)
        {
            int pageSize;
            int.TryParse(context.Request.QueryString["pageSize"], out pageSize);
            if(pageSize== 0)
                pageSize = 25;
            if(pageSize > 1024)
                pageSize = 1024;
            return pageSize;
        }
    }
}