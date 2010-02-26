using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class PutQuery : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/queries"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new []{"PUT"}; }
        }

        protected override void Respond(KayakContext context)
        {
            context.WriteJson(new { viewName = Database.PutView(context.ReadString()) });
        }
    }
}