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
        [Route("ravenfs/admin/{*id}")]
        public async Task<HttpResponseMessage> Put(string id)
        {
            var docKey = "Raven/FileSystems/" + id;
            var dbDoc = await ReadJsonObjectAsync<DatabaseDocument>();
            FileSystemsLandlord.Protect(dbDoc);
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");

            Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

		[HttpDelete]
		[Route("ravenfs/admin/{*id}")]
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
				IOExtensions.DeleteDirectory(configuration.DataDirectory);
				if (configuration.IndexStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.IndexStoragePath);
				if (configuration.JournalsStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.JournalsStoragePath);
			}

			return GetEmptyMessage();
		}
    }
}