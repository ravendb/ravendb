// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
    public class AdminCounterStorageController : BaseAdminController
    {
        [HttpPut]
		[RavenRoute("admin/cs/{*id}")]
		public async Task<HttpResponseMessage> Put(string id)
        {
			var counterNameFormat = CheckNameFormat(id, Database.Configuration.Counter.DataDirectory);
			if (counterNameFormat.Message != null)
			{
				return GetMessageWithObject(new
				{
					Error = counterNameFormat.Message
				}, counterNameFormat.ErrorCode);
			}

			if (Authentication.IsLicensedForCounters == false)
			{
				return GetMessageWithObject(new
				{
					Error = "Your license does not allow the use of Counters!"
				}, HttpStatusCode.BadRequest);
			}

			var docKey = Constants.Counter.Prefix + id;

			var isCounterStorageUpdate = ParseBoolQueryString("update");
			var counterStorage = Database.Documents.Get(docKey, null);
			if (counterStorage != null && isCounterStorageUpdate == false)
            {
				return GetMessageWithString(string.Format("Counter Storage {0} already exists!", id), HttpStatusCode.Conflict);
            }

            var dbDoc = await ReadJsonObjectAsync<CounterStorageDocument>();
            CountersLandlord.Protect(dbDoc);
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");

            Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

		[HttpDelete]
		[RavenRoute("admin/cs/{*id}")]
		public HttpResponseMessage Delete(string id)
		{
			var isHardDeleteNeeded = ParseBoolQueryString("hard-delete");
			var message = DeleteCounterStorage(id, isHardDeleteNeeded);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage();
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
		[RavenRoute("admin/cs/{*id}")]
		public HttpResponseMessage Disable(string id, bool isSettingDisabled)
		{
			var message = ToggleCounterStorageDisabled(id, isSettingDisabled);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage();
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

			var successfullyToggledFileSystems = new List<string>();

			counterStoragesToToggle.ForEach(id =>
			{
				var message = ToggleCounterStorageDisabled(id, isSettingDisabled);
				if (message.ErrorCode == HttpStatusCode.OK)
				{
					successfullyToggledFileSystems.Add(id);
				}
			});

			return GetMessageWithObject(successfullyToggledFileSystems.ToArray());
		}

		private MessageWithStatusCode DeleteCounterStorage(string id, bool isHardDeleteNeeded)
		{
			//get configuration even if the file system is disabled
			var configuration = CountersLandlord.CreateTenantConfiguration(id, true);

			if (configuration == null)
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "Counter storage wasn't found" };

			var docKey = Constants.Counter.Prefix + id;
			Database.Documents.Delete(docKey, null, null);

			if (isHardDeleteNeeded && configuration.RunInMemory == false)
			{
				IOExtensions.DeleteDirectory(configuration.Counter.DataDirectory);
			}

			return new MessageWithStatusCode();
		}

		private MessageWithStatusCode ToggleCounterStorageDisabled(string id, bool isSettingDisabled)
		{
			var docKey = Constants.Counter.Prefix + id;
			var document = Database.Documents.Get(docKey, null);
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
			Database.Documents.Put(docKey, document.Etag, json, new RavenJObject(), null);

			return new MessageWithStatusCode();
		}
    }
}