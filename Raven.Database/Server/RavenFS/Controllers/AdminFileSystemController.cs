// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Controllers.Admin;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Controllers
{
    public class AdminFileSystemController : BaseAdminController
    {
        [HttpPut]
        [Route("fs/admin/{*id}")]
        public async Task<HttpResponseMessage> Put(string id, bool update = false)
        {
            var docKey = "Raven/FileSystems/" + id;
           
            // There are 2 possible ways to call this put. We either want to update a filesystem configuration or we want to create a new one.            
            if (!update)
            {
                // As we are not updating, we should fail when the filesystem already exists.
                var existingFilesystem = Database.Documents.Get(docKey, null);
                if (existingFilesystem != null)                   
                    return GetEmptyMessage(HttpStatusCode.Conflict);
            }

            var dbDoc = await ReadJsonObjectAsync<DatabaseDocument>();
            FileSystemsLandlord.Protect(dbDoc);
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");

            Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

		[HttpDelete]
		[Route("fs/admin/{*id}")]
		public HttpResponseMessage Delete(string id)
		{
			var docKey = "Raven/FileSystems/" + id;

			var configuration = FileSystemsLandlord.CreateTenantConfiguration(id);
			if (configuration == null)
				return GetEmptyMessage();

			Database.Documents.Delete(docKey, null, null);
			bool result;

			if (bool.TryParse(InnerRequest.RequestUri.ParseQueryString()["hard-delete"], out result) && result)
			{
				IOExtensions.DeleteDirectory(configuration.FileSystemDataDirectory);
				if (configuration.IndexStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.IndexStoragePath);
				if (configuration.JournalsStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.JournalsStoragePath);
			}

			return GetEmptyMessage();
		}
    }
}