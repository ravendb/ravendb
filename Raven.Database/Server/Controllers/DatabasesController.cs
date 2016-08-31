using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Raft.Util;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class DatabasesController : BaseDatabaseApiController
    {
        [HttpGet]
        [RavenRoute("databases")]
        public HttpResponseMessage Databases(bool getAdditionalData = false)
        {
            return Resources<DatabaseData>(Constants.Database.Prefix, GetDatabasesData, getAdditionalData);
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

                    var dbName = database.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty);
                    var isDatabaseLoaded = DatabasesLandlord.IsDatabaseLoaded(dbName);
                    DocumentDatabase.ReducedDatabaseStatistics stats = null;
                    if (isDatabaseLoaded)
                    {
                        try
                        {
                            var db = DatabasesLandlord.GetResourceInternal(dbName).Result;
                            if (db != null)
                            {
                                stats = db.ReducedStatistics;
                            }
                        }
                        catch (Exception)
                        {
                            //the database is shutting down or locked
                            //we can ignore this
                        }
                    }

                    return new DatabaseData
                    {
                        Name = dbName,
                        Disabled = database.Value<bool>("Disabled"),
                        IndexingDisabled = GetBooleanSettingStatus(database.Value<RavenJObject>("Settings"), Constants.IndexingDisabled),
                        RejectClientsEnabled = GetBooleanSettingStatus(database.Value<RavenJObject>("Settings"), Constants.RejectClientsModeEnabled),
                        ClusterWide = ClusterManager.IsActive() && !GetBooleanSettingStatus(database.Value<RavenJObject>("Settings"), Constants.Cluster.NonClusterDatabaseMarker),
                        Bundles = bundles,
                        IsAdminCurrentTenant = DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin,
                        IsLoaded = isDatabaseLoaded,
                        Stats = stats
                    };
                }).ToList();
        }

        private class DatabaseData : TenantData
        {
            public bool IndexingDisabled { get; set; }
            public bool RejectClientsEnabled { get; set; }
            public bool ClusterWide { get; set; }
            public DocumentDatabase.ReducedDatabaseStatistics Stats { get; set; }
        }

        /// <summary>
        /// Gets a boolean value out of the setting object.
        /// </summary>
        /// <param name="settingsProperty">Setting as raven object</param>
        /// <param name="propertyName">The property to be fetched</param>
        /// <returns>the value of the requested property as bool, default not found value is false.</returns>
        private static bool GetBooleanSettingStatus(RavenJObject settingsProperty, string propertyName)
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
            }).WithNoCache();
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
            }).WithNoCache();
        }
    }
}
