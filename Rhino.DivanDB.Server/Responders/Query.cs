using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Query : KayakResponder
    {
        public override string UrlPattern
        {
            get { return @"/queries/([\w\d_-]+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET","PUT" }; }
        }

        protected override void Respond(KayakContext context)
        {
            var match = urlMatcher.Match(context.Request.RequestUri);
            var index = match.Groups[1].Value;

            switch (context.Request.Verb)
            {
                case "GET":
                    OnGet(context, index);
                    break;
                case "PUT":
                    context.Response.SetStatusToCreated();
                    context.WriteJson(new
                    {
                        index = Database.PutIndex(index, context.ReadString())
                    });
                    break;
            }
        }

        private void OnGet(KayakContext context, string index)
        {
            var query = context.Request.QueryString["query"];

            if (query == null)
            {
                context.WriteJson(new { index = Database.IndexDefinitionStorage.GetIndexDefinition(index) });
            }
            else
            {
                context.WriteJson(Database.Query(index, query));
            }
        }
    }
}