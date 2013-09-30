using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers.Admin
{
	[RoutePrefix("")]
	public class AdminDatabasesController : BaseAdminController
	{
		[HttpGet("admin/databases/{*id}")]
		public HttpResponseMessage DatabasesGet(string id)
		{
			var docKey = "Raven/Databases/" + id;

			var document = Database.Get(docKey, null);
			if (document == null)
				return GetMessageWithString("Database " + id + " not found", HttpStatusCode.NotFound);

			var dbDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
			dbDoc.Id = id;
			DatabasesLandlord.Unprotect(dbDoc);
			return GetMessageWithObject(dbDoc);
		}

		[HttpPut("admin/databases/{*id}")]
		public async Task DatabasesPut(string id)
		{
			var docKey = "Raven/Databases/" + id;
			var dbDoc = await ReadJsonObjectAsync<DatabaseDocument>();
			DatabasesLandlord.Protect(dbDoc);
			var json = RavenJObject.FromObject(dbDoc);
			json.Remove("Id");

			Database.Put(docKey, null, json, new RavenJObject(), null);
		}

		[HttpDelete("admin/databases/{*id}")]
		public void DatabasesDelete(string id)
		{
			var docKey = "Raven/Databases/" + id;
			var configuration = DatabasesLandlord.CreateTenantConfiguration(id);
			var databasedocument = Database.Get(docKey, null);

			if (configuration == null)
				return;
			Database.Delete(docKey, null, null);
			bool result;

			if (bool.TryParse(Request.RequestUri.ParseQueryString()["hard-delete"], out result) && result)
			{
				IOExtensions.DeleteDirectory(configuration.DataDirectory);
				IOExtensions.DeleteDirectory(configuration.IndexStoragePath);

				if (databasedocument != null)
				{
					var dbDoc = databasedocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
					if (dbDoc != null && dbDoc.Settings.ContainsKey(Constants.RavenLogsPath))
						IOExtensions.DeleteDirectory(dbDoc.Settings[Constants.RavenLogsPath]);
				}
			}
		}
	}
}