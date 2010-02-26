using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class PutView : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/views"; }
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