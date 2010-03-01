using System.Net;

namespace Rhino.DivanDB.Server.Responders
{
    public class Index : RequestResponder
    {
        public override string UrlPattern
        {
            get { return @"/indexes/([\w\d_-]+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET","PUT","DELETE" }; }
        }

        public override void Respond(HttpListenerContext context)
        {
            var match = urlMatcher.Match(context.Request.Url.LocalPath);
            var index = match.Groups[1].Value;

            switch (context.Request.HttpMethod)
            {
                case "GET":
                    OnGet(context, index);
                    break;
                case "PUT":
                    context.SetStatusToCreated("/indexes/" + index);
                    context.WriteJson(new
                    {
                        index = Database.PutIndex(index, context.ReadString())
                    });
                    break;
                case "DELETE":
                    context.SetStatusToDeleted();
                    Database.DeleteIndex(index);
                    context.WriteJson(new { index });
                    break;
            }
        }

        private void OnGet(HttpListenerContext context, string index)
        {
            var query = context.Request.QueryString["query"];

            if (query == null)
            {
                context.WriteJson(new { index = Database.IndexDefinitionStorage.GetIndexDefinition(index) });
            }
            else
            {
                context.WriteJson(Database.Query(index, query, context.GetStart(), context.GetPageSize()));
            }
        }
    }

}