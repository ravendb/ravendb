using System.ComponentModel.Composition;
using Raven.Abstractions.MEF;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class SilverlightUI : AbstractRequestResponder
	{
		[ImportMany]
		public OrderedPartCollection<ISilverlightRequestedAware> SilverlightRequestedAware { get; set; }

		public override string UrlPattern
		{
			get { return @"^/silverlight/(.+\.xap)$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[]{"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			Database.ExtensionsState.GetOrAdd("SilverlightUI.NotifiedAboutSilverlightBeingRequested", s =>
			{
				foreach (var silverlightRequestedAware in SilverlightRequestedAware)
				{
					silverlightRequestedAware.Value.SilverlightWasRequested(Database);
				}
				return true;
			});

			context.WriteEmbeddedFile(Settings.WebDir, "Raven.Studio.xap");
		}

		public override bool IsUserInterfaceRequest
		{
			get { return true; } 
		}
	}
}
