// -----------------------------------------------------------------------
//  <copyright file="AdminCounterStorageController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Counters.Backup;
using Raven.Database.Counters.Replication;
using Raven.Database.Extensions;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
	public class AdminCounterStorageController : BaseAdminCountersApiController
	{
		[HttpGet]
		[RavenRoute("admin/cs/{*counterStorageName}")]
		public async Task<HttpResponseMessage> Get()
		{
			var op = GetQueryStringValue("op");
			if (string.IsNullOrWhiteSpace(op))
				return GetMessageWithString("mandatory 'op' query parameter is missing", HttpStatusCode.BadRequest);

			if (op.Equals("groups-names", StringComparison.InvariantCultureIgnoreCase))
				return await GetNamesAndGroups(CounterName);
			if (op.Equals("summary", StringComparison.InvariantCultureIgnoreCase))
				return await GetSummary(CounterName);

			return GetMessageWithString("'op' query parameter is invalid - must be either group-names or summary", HttpStatusCode.BadRequest);
		}

		private async Task<HttpResponseMessage> GetNamesAndGroups(string id)
		{
			MessageWithStatusCode nameFormateErrorMsg;
			if (IsValidName(id, Counters.Configuration.Counter.DataDirectory, out nameFormateErrorMsg) == false)
			{
				return GetMessageWithObject(new
				{
					Error = nameFormateErrorMsg.Message
				}, nameFormateErrorMsg.ErrorCode);
			}

			if (Authentication.IsLicensedForCounters == false)
			{
				return GetMessageWithObject(new
				{
					Error = "Your license does not allow the use of Counters!"
				}, HttpStatusCode.BadRequest);
			}

			var counterStorage = await CountersLandlord.GetResourceInternal(id).ConfigureAwait(false);
			if (counterStorage == null)
			{
				return GetMessageWithObject(new
				{
					Message = string.Format("Didn't find counter storage (name = {0})", id)
				}, HttpStatusCode.NotFound);
			}

			var counterSummaries = new List<CounterNameGroupPair>();
			using (var reader = counterStorage.CreateReader())
			{
				var groupsAndNames = reader.GetCounterGroups()
					.SelectMany(group => reader.GetCountersSummary(group.Name)
					.Select(x => new CounterNameGroupPair
					{
						Name = x.CounterName,
						Group = group.Name
					}));

				counterSummaries.AddRange(groupsAndNames);
			}

			return GetMessageWithObject(counterSummaries);
		}

		private async Task<HttpResponseMessage> GetSummary(string id)
		{
			MessageWithStatusCode nameFormateErrorMsg;
			if (IsValidName(id, Counters.Configuration.Counter.DataDirectory, out nameFormateErrorMsg) == false)
			{
				return GetMessageWithObject(new
				{
					Error = nameFormateErrorMsg.Message
				}, nameFormateErrorMsg.ErrorCode);
			}

			if (Authentication.IsLicensedForCounters == false)
			{
				return GetMessageWithObject(new
				{
					Error = "Your license does not allow the use of Counters!"
				}, HttpStatusCode.BadRequest);
			}

			var counterStorage = await CountersLandlord.GetResourceInternal(id).ConfigureAwait(false);
			if (counterStorage == null)
			{
				return GetMessageWithObject(new
				{
					Message = string.Format("Didn't find counter storage (name = {0})", id)
				}, HttpStatusCode.NotFound);
			}

			var counterSummaries = new List<CounterSummary>();
			using (var reader = counterStorage.CreateReader())
			{
				counterSummaries.AddRange(
				  reader.GetCounterGroups()
						.SelectMany(x => reader.GetCountersSummary(x.Name)));
			}

			return GetMessageWithObject(counterSummaries);
		}

		[HttpPut]
		[RavenRoute("admin/cs/{*counterStorageName}")]
		public async Task<HttpResponseMessage> Put(string counterStorageName)
		{
			MessageWithStatusCode nameFormatErrorMsg;
			if (IsValidName(counterStorageName, SystemConfiguration.Counter.DataDirectory, out nameFormatErrorMsg) == false)
			{
				return GetMessageWithObject(new
				{
					Error = nameFormatErrorMsg.Message
				}, nameFormatErrorMsg.ErrorCode);
			}

			if (Authentication.IsLicensedForCounters == false)
			{
				return GetMessageWithObject(new
				{
					Error = "Your license does not allow the use of Counters!"
				}, HttpStatusCode.BadRequest);
			}

			var docKey = Constants.Counter.Prefix + counterStorageName;

			var isCounterStorageUpdate = ParseBoolQueryString("update");
			var counterStorage = SystemDatabase.Documents.Get(docKey, null);
			if (counterStorage != null && isCounterStorageUpdate == false)
			{
				return GetMessageWithString(string.Format("Counter Storage {0} already exists!", counterStorageName), HttpStatusCode.Conflict);
			}

			var dbDoc = await ReadJsonObjectAsync<CounterStorageDocument>();
			CountersLandlord.Protect(dbDoc);
			var json = RavenJObject.FromObject(dbDoc);
			json.Remove("Id");

			SystemDatabase.Documents.Put(docKey, null, json, new RavenJObject(), null);

			return GetEmptyMessage(HttpStatusCode.Created);
		}

		[HttpDelete]
		[RavenRoute("admin/cs/{*counterStorageName}")]
		public HttpResponseMessage Delete(string counterStorageName)
		{
			var isHardDeleteNeeded = ParseBoolQueryString("hard-delete");
			var message = DeleteCounterStorage(counterStorageName, isHardDeleteNeeded);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpDelete]
		[RavenRoute("admin/cs/batch-delete")]
		public HttpResponseMessage BatchDelete()
		{
			string[] counterStoragesToDelete = GetQueryStringValues("ids");
			if (counterStoragesToDelete == null)
			{
				return GetMessageWithString("No counter storages to delete!", HttpStatusCode.BadRequest);
			}

			var isHardDeleteNeeded = ParseBoolQueryString("hard-delete");
			var successfullyDeletedDatabase = new List<string>();

			counterStoragesToDelete.ForEach(id =>
			{
				var message = DeleteCounterStorage(id, isHardDeleteNeeded);
				if (message.ErrorCode == HttpStatusCode.OK)
				{
					successfullyDeletedDatabase.Add(id);
				}
			});

			return GetMessageWithObject(successfullyDeletedDatabase.ToArray());
		}

		[HttpPost]
		[RavenRoute("admin/cs/{*counterStorageName}")]
		public HttpResponseMessage Disable(string counterStorageName, bool isSettingDisabled)
		{
			var message = ToggleCounterStorageDisabled(counterStorageName, isSettingDisabled);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpPost]
		[RavenRoute("admin/cs/batch-toggle-disable")]
		public HttpResponseMessage ToggleDisable(bool isSettingDisabled)
		{
			string[] counterStoragesToToggle = GetQueryStringValues("ids");
			if (counterStoragesToToggle == null)
			{
				return GetMessageWithString("No counter storages to toggle!", HttpStatusCode.BadRequest);
			}

			var successfullyToggledCounters = new List<string>();

			counterStoragesToToggle.ForEach(id =>
			{
				var message = ToggleCounterStorageDisabled(id, isSettingDisabled);
				if (message.ErrorCode == HttpStatusCode.OK)
				{
					successfullyToggledCounters.Add(id);
				}
			});

			return GetMessageWithObject(successfullyToggledCounters.ToArray());
		}

		private MessageWithStatusCode DeleteCounterStorage(string id, bool isHardDeleteNeeded)
		{
			//get configuration even if the counters is disabled
			var configuration = CountersLandlord.CreateTenantConfiguration(id, true);

			if (configuration == null)
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "Counter storage wasn't found" };

			var docKey = Constants.Counter.Prefix + id;
			SystemDatabase.Documents.Delete(docKey, null, null);

			if (isHardDeleteNeeded && configuration.RunInMemory == false)
			{
				IOExtensions.DeleteDirectory(configuration.Counter.DataDirectory);
			}

			return new MessageWithStatusCode();
		}

		private MessageWithStatusCode ToggleCounterStorageDisabled(string id, bool isSettingDisabled)
		{
			var docKey = Constants.Counter.Prefix + id;
			var document = SystemDatabase.Documents.Get(docKey, null);
			if (document == null)
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "Counter storage " + id + " wasn't found" };

			var doc = document.DataAsJson.JsonDeserialization<CounterStorageDocument>();
			if (doc.Disabled == isSettingDisabled)
			{
				var state = isSettingDisabled ? "disabled" : "enabled";
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.BadRequest, Message = "Counter storage " + id + " is already " + state };
			}

			doc.Disabled = !doc.Disabled;
			var json = RavenJObject.FromObject(doc);
			json.Remove("Id");
			SystemDatabase.Documents.Put(docKey, document.Etag, json, new RavenJObject(), null);

			return new MessageWithStatusCode();
		}

		[HttpPost]
		[RavenRoute("cs/{counterStorageName}/admin/backup")]
		public async Task<HttpResponseMessage> Backup()
		{
			var backupRequest = await ReadJsonObjectAsync<CounterStorageBackupRequest>();
			var incrementalBackup = ParseBoolQueryString("incremental");

			if (backupRequest.CounterStorageDocument == null && Counters.Name != null)
			{
				var jsonDocument = DatabasesLandlord.SystemDatabase.Documents.Get(Constants.Counter.Prefix + Counters.Name, null);
				if (jsonDocument != null)
				{
					backupRequest.CounterStorageDocument = jsonDocument.DataAsJson.JsonDeserialization<CounterStorageDocument>();
					CountersLandlord.Unprotect(backupRequest.CounterStorageDocument);
					backupRequest.CounterStorageDocument.StoreName = Counters.Name;
				}
			}

			using (var reader = Counters.CreateReader())
			{
				var backupStatus = reader.GetBackupStatus();
				if (backupStatus != null && backupStatus.IsRunning)
					throw new InvalidOperationException("Backup is already running");
			}

			if (incrementalBackup &&
				Counters.Configuration.Storage.Voron.AllowIncrementalBackups == false)
			{
				throw new InvalidOperationException("In order to run incremental backups using Voron you must have the appropriate setting key (Raven/Voron/AllowIncrementalBackups) set to true");
			}

			using (var writer = Counters.CreateWriter())
			{
				writer.SaveBackupStatus(new BackupStatus
				{
					Started = SystemTime.UtcNow,
					IsRunning = true,
				});
			}

			var backupOperation = new BackupOperation(Counters, DatabasesLandlord.SystemDatabase.Configuration.DataDirectory,
				backupRequest.BackupLocation, Counters.Environment, incrementalBackup, backupRequest.CounterStorageDocument);

#pragma warning disable 4014
			Task.Factory.StartNew(backupOperation.Execute);
#pragma warning restore 4014

			return GetEmptyMessage(HttpStatusCode.Accepted);
		}

		[HttpPost]
		[RavenRoute("cs/{counterStorageName}/admin/replication/topology/view")]
		public Task<HttpResponseMessage> ReplicationTopology()
		{
			var topologyDiscoverer = new CountersReplicationTopologyDiscoverer(Counters, new RavenJArray(), 10, Log);
			var node = topologyDiscoverer.Discover();
			var topology = node.Flatten();

			return GetMessageWithObjectAsTask(topology);
		}
	}
}