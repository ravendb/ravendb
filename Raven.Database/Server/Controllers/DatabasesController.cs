using System;
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
		[HttpGet][Route("databases")]
		public HttpResponseMessage Databases()
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

			List<string> approvedDatabases = null;
		    var start = GetStart();
			var nextPageStart = start; // will trigger rapid pagination
			var databases = Database.Documents.GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, start,
																	GetPageSize(Database.Configuration.MaxPageSize), CancellationToken.None, ref nextPageStart);
			var data = databases
				.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty))
				.ToArray();

			if (DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
			{
				var user = User;
				if (user == null)
					return null;

				if (user.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode) == false)
				{
					var authorizer = (MixedModeRequestAuthorizer)this.ControllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

					approvedDatabases = authorizer.GetApprovedDatabases(user, this, data);
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
				data = data.Where(s => approvedDatabases.Contains(s)).ToArray();

			var msg = GetMessageWithObject(data);
			WriteHeaders(new RavenJObject(), lastDocEtag, msg);

			return msg;
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