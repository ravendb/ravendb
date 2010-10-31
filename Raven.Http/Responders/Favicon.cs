using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Http.Responders
{
    public class Favicon : AbstractRequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/favicon.ico$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }

        public override void Respond(IHttpContext context)
        {
            context.WriteEmbeddedFile(ResourceStore.GetType().Assembly, Settings.WebDir, "favicon.ico");
        }

        public override bool IsUserInterfaceRequest
        {
            get
            {
                return true;
            }
        }
    }
}
