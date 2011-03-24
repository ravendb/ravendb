using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;
using Raven.Http.Plugins;

namespace Raven.Http.Responders
{
	public class SilverlightUI : AbstractRequestResponder
	{
		[Import(AllowDefault = true)]
		public ISilverlightRequestedAware SilverlightRequestedAware { get; set; }

		private bool notifiedAboutSilverlightBeingRequested;

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
			if(notifiedAboutSilverlightBeingRequested == false)
			{
				lock (this)
				{
					if (notifiedAboutSilverlightBeingRequested == false && 
						SilverlightRequestedAware != null)

						SilverlightRequestedAware.SilverlightWasRequested(ResourceStore);

					notifiedAboutSilverlightBeingRequested = true;
				}
			}
			var match = urlMatcher.Match(context.GetRequestUrl());
			var fileName = match.Groups[1].Value;
			var paths = GetPaths(fileName);
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

		private IEnumerable<string> GetPaths(string fileName)
		{
			//local path
			yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, fileName);
			// dev path
			yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\Raven.Studio\bin\debug", fileName);
			// web ui path
			yield return Path.Combine(this.ResourceStore.Configuration.WebDir, fileName);
		}
	}
}