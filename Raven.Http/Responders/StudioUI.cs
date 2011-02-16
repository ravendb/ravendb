namespace Raven.Http.Responders
{
	using Abstractions;
	using Extensions;

	public class StudioUI : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/studio/"; }
		}

		public override bool IsUserInterfaceRequest
		{
			get { return true; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var docPath = context.GetRequestUrl().Replace("/studio/", "");
			if(string.IsNullOrEmpty(docPath)) docPath = "studio.html";
			context.WriteEmbeddedFile(ResourceStore.GetType().Assembly, Settings.WebDir, docPath);
		}
	}
}