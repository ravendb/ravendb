using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Web.Http;
using Raven.Database.Plugins.Builtins;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class SilverlightController : RavenDbApiController
	{
		[HttpGet]
		[Route("silverlight/ensureStartup")]
		[Route("databases/{databaseName}/silverlight/ensureStartup")]
		public HttpResponseMessage SilverlightEnsureStartup()
		{
			Database.ExtensionsState.GetOrAdd("SilverlightUI.NotifiedAboutSilverlightBeingRequested", s =>
			{
				var skipCreatingStudioIndexes = Database.Configuration.Settings["Raven/SkipCreatingStudioIndexes"];
				if (string.IsNullOrEmpty(skipCreatingStudioIndexes) == false &&
					"true".Equals(skipCreatingStudioIndexes, StringComparison.OrdinalIgnoreCase))
					return true;

				new CreateSilverlightIndexes().SilverlightWasRequested(Database);
				return true;
			});

			return GetMessageWithObject(new { ok = true });
		}

		[HttpGet][Route("silverlight/{*id}")]
		public HttpResponseMessage SilverlightUi(string id)
		{
			if (id.Contains(".xap") == false)
				return GetEmptyMessage();
			Database.ExtensionsState.GetOrAdd("SilverlightUI.NotifiedAboutSilverlightBeingRequested", s =>
			{
				new CreateSilverlightIndexes().SilverlightWasRequested(Database);
				return true;
			});

		    string matchingPath = null;

			var fileName = id;
			var paths = GetPaths(fileName, Database.Configuration.WebDir);
			
			matchingPath = paths.FirstOrDefault(path =>
			{
				try
				{
					return File.Exists(path);
				}
				catch (Exception)
				{
					return false;
				}
			});

			return matchingPath != null ? WriteFile(matchingPath) : WriteEmbeddedFile(DatabasesLandlord.SystemConfiguration.WebDir, "Raven.Database.Server.WebUI", null, "Raven.Studio.xap");
		}

		public static IEnumerable<string> GetPaths(string fileName, string webDir)
		{
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
				try
				{
					if (Directory.Exists(option) == false)
						continue;
				}
				catch (Exception)
				{
					yield break;
				}
				string[] directories;
				try
				{
					directories = Directory.GetDirectories(option, "RavenDB.Embedded*");
				}
				catch (Exception)
				{
					yield break;
				}
				foreach (var dir in directories.OrderByDescending(x => x))
				{
					var contentDir = Path.Combine(dir, "content");
					bool exists;
					try
					{
						exists = Directory.Exists(contentDir);
					}
					catch (Exception)
					{
						continue;
					}
					if (exists)
						yield return contentDir;
				}
			}
		}
	}
}