using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Static : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/static/(.+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET", "PUT", "DELETE"}; }
        }

        protected override void Respond(KayakContext context)
        {
            var match = urlMatcher.Match(context.Request.Path);
            var filename = match.Groups[1].Value;
            switch (context.Request.Verb)
            {
                case "GET":
                    context.WriteData(Database.GetStatic(filename));
                    break;
                case "PUT":
                    Database.PutStatic(filename, context.ReadData());
                    context.SetStatusToCreated("/static/"+filename);
                    break;
                case "DELETE":
                    Database.DeleteStatic(filename);
                    context.SetStatusToDeleted();
                    break;
            }
        }
    }
}