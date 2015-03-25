using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Raft.Util;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class DatabasesController : RavenDbApiController
	{
	    [HttpGet]
		[RavenRoute("databases")]
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
			var databases = Database.Documents.GetDocumentsWithIdStartingWith(Constants.Database.Prefix, null, null, start,
										GetPageSize(Database.Configuration.MaxPageSize), CancellationToken.None, ref nextPageStart);

			var databasesData = GetDatabasesData(databases);
			var databasesNames = databasesData.Select(databaseObject => databaseObject.Name).ToArray();

			List<string> approvedDatabases = null;
			if (SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
			{
                var authorizer = (MixedModeRequestAuthorizer)ControllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

                HttpResponseMessage authMsg;
                if (authorizer.TryAuthorize(this, out authMsg) == false)
                    return authMsg;

			    var user = authorizer.GetUser(this);
				if (user == null)
					return authMsg;

				if (user.IsAdministrator(SystemConfiguration.AnonymousUserAccessMode) == false)
				{
					approvedDatabases = authorizer.GetApprovedResources(user, this, databasesNames);
				}

				databasesData.ForEach(x =>
				{
					var principalWithDatabaseAccess = user as PrincipalWithDatabaseAccess;
					if (principalWithDatabaseAccess != null)
					{
						var isAdminGlobal = principalWithDatabaseAccess.IsAdministrator(SystemConfiguration.AnonymousUserAccessMode);
						x.IsAdminCurrentTenant = isAdminGlobal || principalWithDatabaseAccess.IsAdministrator(Database);
					}
					else
					{
						x.IsAdminCurrentTenant = user.IsAdministrator(x.Name);
					}
				});
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

		private List<DatabaseData> GetDatabasesData(IEnumerable<RavenJToken> databases)
		{
			return databases
				.Select(database =>
				{
					var bundles = new string[] {};
					var settings = database.Value<RavenJObject>("Settings");
					if (settings != null)
					{
						var activeBundles = settings.Value<string>("Raven/ActiveBundles");
						if (activeBundles != null)
						{
							bundles = activeBundles.Split(';');
						}
					}

					return new DatabaseData
					{
						Name = database.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty),
						Disabled = database.Value<bool>("Disabled"),
						IndexingDisabled = GetBooleanSettingStatus(database.Value<RavenJObject>("Settings"), Constants.IndexingDisabled),
						RejectClientsEnabled = GetBooleanSettingStatus(database.Value<RavenJObject>("Settings"), Constants.RejectClientsModeEnabled),
						ClusterWide = ClusterManager.IsActive() && !GetBooleanSettingStatus(database.Value<RavenJObject>("Settings"), Constants.Cluster.NonClusterDatabaseMarker),
						Bundles = bundles,
						IsAdminCurrentTenant = DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin,
					};
				}).ToList();
		}

		private class DatabaseData : TenantData
		{
			public bool IndexingDisabled { get; set; }
			public bool RejectClientsEnabled { get; set; }
			public bool ClusterWide { get; set; }
		}

        /// <summary>
        /// Gets a boolean value out of the setting object.
        /// </summary>
        /// <param name="settingsProperty">Setting as raven object</param>
        /// <param name="propertyName">The property to be fetched</param>
        /// <returns>the value of the requested property as bool, default not found value is false.</returns>
        private bool GetBooleanSettingStatus(RavenJObject settingsProperty,string propertyName)
	    {
	        if (settingsProperty == null)
	            return false;

            var propertyStatusString = settingsProperty.Value<string>(propertyName);
            if (propertyStatusString == null)
                return false;

            bool propertyStatus;
            if(bool.TryParse(propertyStatusString, out propertyStatus))
                return propertyStatus;
            return false;
	    }

	    [HttpGet]
		[RavenRoute("database/size")]
		[RavenRoute("databases/{databaseName}/database/size")]
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
		[RavenRoute("database/storage/sizes")]
		[RavenRoute("databases/{databaseName}/database/storage/sizes")]
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