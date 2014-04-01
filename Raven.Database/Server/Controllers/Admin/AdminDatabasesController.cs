using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Http;
using Amazon.SQS.Model;
using Mono.CSharp;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers.Admin
{
	[RoutePrefix("")]
	public class AdminDatabasesController : BaseAdminController
	{
		[HttpGet][Route("admin/databases/{*id}")]
		public HttpResponseMessage DatabasesGet(string id)
		{
			if (IsSystemDatabase(id))
			{
				//fetch fake (empty) system database document
				var systemDatabaseDocument = new DatabaseDocument { Id = Constants.SystemDatabase };
				return GetMessageWithObject(systemDatabaseDocument);
			}

			var docKey = "Raven/Databases/" + id;

			var document = Database.Documents.Get(docKey, null);
			if (document == null)
				return GetMessageWithString("Database " + id + " not found", HttpStatusCode.NotFound);

			var dbDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
			dbDoc.Id = id;
			DatabasesLandlord.Unprotect(dbDoc);
			return GetMessageWithObject(dbDoc);
		}

		private Tuple<string, HttpStatusCode> CheckInput(string databaseName)
		{
			string errorMessage = null;
			HttpStatusCode errorCode = HttpStatusCode.BadRequest;

			if (databaseName == null)
			{
				errorMessage = "An empty name is forbidden for use!";
			}
			else if (databaseName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
			{
				errorMessage = string.Format("The name '{0}' contains charaters that are forbidden for use!", databaseName);
			}
			else if (Array.IndexOf(Constants.WindowsReservedFileNames, databaseName.ToLower()) >= 0){
				errorMessage = string.Format("The name '{0}' is forbidden for use!", databaseName);
			}
			else if ((Environment.OSVersion.Platform == PlatformID.Unix) && (databaseName.Length > Constants.LinuxMaxFileNameLength) && (Database.Configuration.DataDirectory.Length + databaseName.Length > Constants.LinuxMaxPath))
			{
				int theoreticalMaxFileNameLength = Constants.LinuxMaxPath - Database.Configuration.DataDirectory.Length;
				int maxfileNameLength = (theoreticalMaxFileNameLength > Constants.LinuxMaxFileNameLength) ? Constants.LinuxMaxFileNameLength : theoreticalMaxFileNameLength;
				errorMessage = string.Format("Invalid name for a database! Databse name cannot exceed {0} characters", maxfileNameLength);
			}
			else if (Path.Combine(Database.Configuration.DataDirectory, databaseName).Length > Constants.WindowsMaxPath)
			{
				int maxfileNameLength = Constants.WindowsMaxPath - Database.Configuration.DataDirectory.Length;
				errorMessage = string.Format("Invalid name for a database! Databse name cannot exceed {0} characters", maxfileNameLength);
			}
			else if (IsSystemDatabase(databaseName))
			{
				errorMessage = "System Database document cannot be changed";
				errorCode = HttpStatusCode.Forbidden;
			}

			return new Tuple<string, HttpStatusCode>(errorMessage, errorCode);
		}

		[HttpPut]
		[Route("admin/databases/{*id}")]
		public async Task<HttpResponseMessage> DatabasesPut(string id)
		{
			Tuple<string, HttpStatusCode> message = CheckInput(id);
			if (message.Item1 != null)
			{
				return GetMessageWithString(message.Item1, message.Item2);
			}

			var docKey = "Raven/Databases/" + id;
			var existingDatabase = Database.Documents.Get(docKey, null);
			if (existingDatabase != null)
			{
				return GetMessageWithString(string.Format("Database with the name '{0}' already exists", id), HttpStatusCode.BadRequest);
			}

			var dbDoc = await ReadJsonObjectAsync<DatabaseDocument>();
			if (dbDoc.Settings.ContainsKey("Bundles") && dbDoc.Settings["Bundles"].Contains("Encryption"))
			{
				if (!dbDoc.SecuredSettings.ContainsKey(Constants.EncryptionKeySetting) ||
				    !dbDoc.SecuredSettings.ContainsKey(Constants.AlgorithmTypeSetting))
				{
					return GetMessageWithString(string.Format("Failed to create '{0}' database, becuase of not valid encryption configuration.", id), HttpStatusCode.BadRequest);
				}
			}

			DatabasesLandlord.Protect(dbDoc);
			var json = RavenJObject.FromObject(dbDoc);
			json.Remove("Id");

			Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

			return GetEmptyMessage();
		}

		[HttpPost]
		[Route("admin/databases/{*id}")]
		public async Task<HttpResponseMessage> DatabasePost(string id)
		{
			var docKey = "Raven/Databases/" + id;
			var existingDatabase = Database.Documents.Get(docKey, null);
			if (existingDatabase == null)
			{
				return GetMessageWithString(string.Format("Database with the name '{0}' doesn't exist", id), HttpStatusCode.BadRequest);
			}

			var dbDoc = await ReadJsonObjectAsync<DatabaseDocument>();
			if (dbDoc.Settings.ContainsKey("Bundles") && dbDoc.Settings["Bundles"].Contains("Encryption"))
			{
				if (!dbDoc.SecuredSettings.ContainsKey(Constants.EncryptionKeySetting) ||
				    !dbDoc.SecuredSettings.ContainsKey(Constants.AlgorithmTypeSetting))
				{
					return GetMessageWithString(string.Format("Failed to modify '{0}' database, becuase of not valid encryption configuration.", id), HttpStatusCode.BadRequest);
				}
			}

			DatabasesLandlord.Protect(dbDoc);
			var json = RavenJObject.FromObject(dbDoc);
			json.Remove("Id");

			Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

			return GetEmptyMessage();
		}


		[HttpDelete][Route("admin/databases/{*id}")]
		public HttpResponseMessage DatabasesDelete(string id)
		{
			if (IsSystemDatabase(id))
				return GetMessageWithString("System Database document cannot be deleted", HttpStatusCode.Forbidden);

			var docKey = "Raven/Databases/" + id;
			var configuration = DatabasesLandlord.CreateTenantConfiguration(id);
			var databasedocument = Database.Documents.Get(docKey, null);

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