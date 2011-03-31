using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Database.Impl;
using Raven.Http.Abstractions;
using Raven.Http.Plugins;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
	public class SilverlightEnsuresStartup : RequestResponder
	{
		[ImportMany]
		public OrderedPartCollection<ISilverlightRequestedAware> SilverlightRequestedAware { get; set; }


		public override string UrlPattern
		{
			get { return @"^/silverlight/ensureStartup$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[]{"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			ResourceStore.ExternalState.GetOrAddAtomically("SilverlightUI.NotifiedAboutSilverlightBeingRequested", s =>
			{
				foreach (var silverlightRequestedAware in SilverlightRequestedAware)
				{
					silverlightRequestedAware.Value.SilverlightWasRequested(ResourceStore);
				}
				return true;
			});

			context.WriteJson(new {ok = true});
		}
	}
}