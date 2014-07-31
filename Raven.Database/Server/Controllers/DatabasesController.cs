using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Security;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class DatabasesController : RavenDbApiController
	{
		[HttpGet]
		[Route("databases")]
		public HttpResponseMessage Databases(bool getAdditionalData = false)
		{
			if (EnsureSystemDatabase() == false)
				return
					GetMessageWithString(
						"The request '" + InnerRequest.RequestUri.AbsoluteUri + "' can only be issued on the system database",
						HttpStatusCode.BadRequest);

			// This method is NOT secured, and anyone can access it.
			// Because of that, we need to provide explicit security here.

			// Anonymous Access - All / Get / Admin
			// Show all dbs

			// Anonymous Access - None
			// Show only the db that you have access to (read / read-write / admin)

			// If admin, show all dbs

			var start = GetStart();
			var nextPageStart = start; // will trigger rapid pagination
			var databases = Database.Documents.GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, start,
										GetPageSize(Database.Configuration.MaxPageSize), CancellationToken.None, ref nextPageStart);

			var databasesData = databases
				.Select(database =>
					new
					{
						Name = database.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty),
						Disabled = database.Value<bool>("Disabled")
					}).ToList();

			var databasesNames = databasesData.Select(databaseObject => databaseObject.Name).ToArray();

			List<string> approvedDatabases = null;
			if (DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
			{
				var user = User;
				if (user == null)
					return null;

				if (user.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode) == false)
				{
					var authorizer = (MixedModeRequestAuthorizer)this.ControllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

					approvedDatabases = authorizer.GetApprovedResources(user, this, databasesNames);
				}
			}

			var lastDocEtag = Etag.Empty;
			Database.TransactionalStorage.Batch(accessor =>
			{
				lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
			});

			if (MatchEtag(lastDocEtag))
				return GetEmptyMessage(HttpStatusCode.NotModified);

			if (approvedDatabases != null)
			{
				databasesData = databasesData.Where(databaseData => approvedDatabases.Contains(databaseData.Name)).ToList();
				databasesNames = databasesNames.Where(databaseName => approvedDatabases.Contains(databaseName)).ToArray();
			}

			var responseMessage = getAdditionalData ? GetMessageWithObject(databasesData) : GetMessageWithObject(databasesNames);
			WriteHeaders(new RavenJObject(), lastDocEtag, responseMessage);
			return responseMessage;
		}

		[HttpGet]
		[Route("database/size")]
		[Route("databases/{databaseName}/database/size")]
		public HttpResponseMessage DatabaseSize()
		{
			var totalSizeOnDisk = Database.GetTotalSizeOnDisk();
			return GetMessageWithObject(new
			{
				DatabaseSize = totalSizeOnDisk,
				DatabaseSizeHumane = SizeHelper.Humane(totalSizeOnDisk)
			});
		}

		[HttpGet]
		[Route("database/storage/sizes")]
		[Route("databases/{databaseName}/database/storage/sizes")]
		public HttpResponseMessage DatabaseStorageSizes()
		{
			var indexStorageSize = Database.GetIndexStorageSizeOnDisk();
			var transactionalStorageSize = Database.GetTransactionalStorageSizeOnDisk();
			var totalDatabaseSize = indexStorageSize + transactionalStorageSize.AllocatedSizeInBytes;
			return GetMessageWithObject(new
			{
				TransactionalStorageAllocatedSize = transactionalStorageSize.AllocatedSizeInBytes,
				TransactionalStorageAllocatedSizeHumaneSize = SizeHelper.Humane(transactionalStorageSize.AllocatedSizeInBytes),
				TransactionalStorageUsedSize = transactionalStorageSize.UsedSizeInBytes,
				TransactionalStorageUsedSizeHumaneSize = SizeHelper.Humane(transactionalStorageSize.UsedSizeInBytes),
				IndexStorageSize = indexStorageSize,
				IndexStorageSizeHumane = SizeHelper.Humane(indexStorageSize),
				TotalDatabaseSize = totalDatabaseSize,
				TotalDatabaseSizeHumane = SizeHelper.Humane(totalDatabaseSize),
			});
		}


	}
}