using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Extensions;
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
			ResourceStore.ExtensionsState.GetOrAddAtomically("SilverlightUI.NotifiedAboutSilverlightBeingRequested", s =>
			{
				foreach (var silverlightRequestedAware in SilverlightRequestedAware)
				{
					silverlightRequestedAware.Value.SilverlightWasRequested(ResourceStore);
				}
				return true;
			});

			var match = urlMatcher.Match(context.GetRequestUrl());
			var fileName = match.Groups[1].Value;
			var paths = GetPaths(fileName, ResourceStore.Configuration.WebDir);
			var matchingPath = paths.FirstOrDefault(File.Exists);
			if (matchingPath != null)
			{
				context.WriteFile(matchingPath);
				return;
			}
			context.SetStatusToNotFound();
		}

		public override bool IsUserInterfaceRequest
		{
			get { return true; } 
		}

		public static IEnumerable<string> GetPaths(string fileName, string webDir)
		{
			// dev path
			yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\Raven.Studio\bin\debug", fileName);
			// dev path
			yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\Raven.Studio\bin\debug", fileName);
			//local path
			yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, fileName);
			// web ui path
			yield return Path.Combine(webDir, fileName);
		}
	}
}