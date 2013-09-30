using System;
using System.ComponentModel.Composition;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.MEF;
using Raven.Database.Plugins;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class SilverlightController : RavenApiController
	{
		[ImportMany]
		public OrderedPartCollection<ISilverlightRequestedAware> SilverlightRequestedAware { get; set; }

		[HttpGet("silverlight/ensureStartup")]
		public HttpResponseMessage SilverlightEnsureStartup()
		{
			Database.ExtensionsState.GetOrAdd("SilverlightUI.NotifiedAboutSilverlightBeingRequested", s =>
			{
				var skipCreatingStudioIndexes = Database.Configuration.Settings["Raven/SkipCreatingStudioIndexes"];
				if (string.IsNullOrEmpty(skipCreatingStudioIndexes) == false &&
					"true".Equals(skipCreatingStudioIndexes, StringComparison.OrdinalIgnoreCase))
					return true;

				foreach (var silverlightRequestedAware in SilverlightRequestedAware)
				{
					silverlightRequestedAware.Value.SilverlightWasRequested(Database);
				}
				return true;
			});

			return GetMessageWithObject(new { ok = true });
		}
	}
}
