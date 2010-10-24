using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class RavenUI : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/raven/"; }
		}

        public override bool IsUserInterfaceRequest
        {
            get
            {
                return true;
            }
        }

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var docPath = context.GetRequestUrl().Replace("/raven/", "");
			context.WriteEmbeddedFile(Settings.WebDir, docPath);
		}

		
	}
}
