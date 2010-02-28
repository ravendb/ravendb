using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Indexes : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/indexes/?$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET"}; }
        }

        protected override void Respond(KayakContext context)
        {
            switch (context.Request.Verb)
            {
                case "GET":
                    context.WriteJson(Database.GetIndexes(GetStart(context), GetPageSize(context)));
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
            if (pageSize == 0)
                pageSize = 25;
            if (pageSize > 1024)
                pageSize = 1024;
            return pageSize;
        }
    }
}