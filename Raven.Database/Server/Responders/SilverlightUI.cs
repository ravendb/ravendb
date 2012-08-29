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
			Database.ExtensionsState.GetOrAdd("SilverlightUI.NotifiedAboutSilverlightBeingRequested", s =>
			{
				foreach (var silverlightRequestedAware in SilverlightRequestedAware)
				{
					silverlightRequestedAware.Value.SilverlightWasRequested(Database);
				}
				return true;
			});

			var match = urlMatcher.Match(context.GetRequestUrl());
			var fileName = match.Groups[1].Value;
			var paths = GetPaths(fileName, Database.Configuration.WebDir);
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
			//local path, bin folder
			yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bin", fileName);

			// web ui path
			yield return Path.Combine(webDir, fileName);

			var options = new[]
			              	{
			              		@"..\..\..\packages", // assuming we are in slnDir\Project.Name\bin\debug 		
			              		@"..\..\packages"
			              	};
			foreach (var option in options)
			{
				if (Directory.Exists(option) == false)
					continue;
				foreach (var dir in Directory.GetDirectories(option, "RavenDB.Embedded*").OrderByDescending(x => x))
				{
					var contentDir = Path.Combine(dir, "content");
					if (Directory.Exists(contentDir))
						yield return contentDir;
				}
			}
		}
	}
}
