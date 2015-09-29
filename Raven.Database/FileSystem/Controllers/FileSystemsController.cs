using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Database.Extensions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

// -----------------------------------------------------------------------
//  <copyright file="FileSystemsController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Database.FileSystem.Controllers
{
	public class FileSystemsController : BaseDatabaseApiController
	{
		[HttpGet]
		[RavenRoute("fs")]
		public HttpResponseMessage FileSystems(bool getAdditionalData = false)
		{
			return Resources<FileSystemData>(Constants.FileSystem.Prefix, GetFileSystemsData, getAdditionalData);
		}

		private List<FileSystemData> GetFileSystemsData(IEnumerable<RavenJToken> fileSystems)
		{
			return fileSystems
				.Select(fileSystem =>
				{
					var bundles = new string[] { };
					var settings = fileSystem.Value<RavenJObject>("Settings");
					if (settings != null)
					{
						var activeBundles = settings.Value<string>("Raven/ActiveBundles");
						if (activeBundles != null)
						{
							bundles = activeBundles.Split(';');
						}
					}
					var fsName = fileSystem.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(Constants.FileSystem.Prefix, string.Empty);
                    return new FileSystemData
					{
						Name = fsName,
						Disabled = fileSystem.Value<bool>("Disabled"),
						Bundles = bundles,
						IsAdminCurrentTenant = true,
						IsLoaded = FileSystemsLandlord.IsFileSystemLoaded(fsName)
					};
				}).ToList();
		}

		private class FileSystemData : TenantData
		{
		}

		[HttpGet]
		[RavenRoute("fs/status")]
		public HttpResponseMessage Status()
		{
			string status = "ready";
			if (!RavenFileSystem.IsRemoteDifferentialCompressionInstalled)
				status = "install-rdc";

			var result = new { Status = status };

			return GetMessageWithObject(result).WithNoCache();
		}

		[HttpGet]
		[RavenRoute("fs/stats")]
		public async Task<HttpResponseMessage> Stats()
		{
			var fileSystemsDocument = GetResourcesDocuments(Constants.FileSystem.Prefix);
			var fileSystemsData = GetFileSystemsData(fileSystemsDocument);
			var fileSystemsNames = fileSystemsData.Select(fileSystemObject => fileSystemObject.Name).ToArray();

			var stats = new List<FileSystemStats>();
			foreach (var fileSystemName in fileSystemsNames)
			{
				Task<RavenFileSystem> fsTask;
				if (!FileSystemsLandlord.TryGetFileSystem(fileSystemName, out fsTask)) // we only care about active file systems
					continue;

				if (fsTask.IsCompleted == false)
					continue; // we don't care about in process of starting file systems

				var ravenFileSystem = await fsTask.ConfigureAwait(false);
				var fsStats = ravenFileSystem.GetFileSystemStats();
				stats.Add(fsStats);
			}

			return GetMessageWithObject(stats).WithNoCache();
		}
	}
}