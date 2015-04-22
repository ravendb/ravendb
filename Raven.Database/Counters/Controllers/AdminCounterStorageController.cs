// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Counters.Controllers
{
    public class AdminCounterStorageController : BaseAdminController
    {
        [HttpPut]
		[RavenRoute("admin/cs/{*counterStorageName}")]
        public async Task<HttpResponseMessage> Put(string counterStorageName)
        {
            var docKey = Constants.Counter.Prefix + counterStorageName;

			var isCounterStorageUpdate = CheckQueryStringParameterResult("update");
			if (IsCounterStorageNameExists(counterStorageName) && !isCounterStorageUpdate)
            {
				return GetMessageWithString(string.Format("Counter Storage {0} already exists!", counterStorageName), HttpStatusCode.Conflict);
            }

            var dbDoc = await ReadJsonObjectAsync<CounterStorageDocument>();
            CountersLandlord.Protect(dbDoc);
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");

            Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

	    private bool CheckQueryStringParameterResult(string parameterName)
	    {
		    bool result;
		    return bool.TryParse(InnerRequest.RequestUri.ParseQueryString()[parameterName], out result) && result;
	    }

		[HttpDelete]
		[RavenRoute("admin/cs/{*counterStorageName}")]
		public HttpResponseMessage Delete(string counterStorageName)
		{
			var docKey = Constants.Counter.Prefix + counterStorageName;
            var configuration = CountersLandlord.CreateTenantConfiguration(counterStorageName);

			if (configuration == null)
				return GetEmptyMessage();

            if (!IsCounterStorageNameExists(counterStorageName))
            {
                return GetMessageWithString(string.Format("Counter Storage {0} doesn't exist!", counterStorageName), HttpStatusCode.NotFound);
            }

			Database.Documents.Delete(docKey, null, null);

			bool isHardDeleteNeeded = CheckQueryStringParameterResult("hard-delete");
			if (isHardDeleteNeeded)
			{
				IOExtensions.DeleteDirectory(configuration.CountersDataDirectory);
			}

			return GetEmptyMessage();
		}

		[HttpDelete]
		[RavenRoute("admin/cs/batch-delete")]
		public HttpResponseMessage BatchDelete()
		{
			throw new NotImplementedException();
		}

		[HttpPost]
		[RavenRoute("admin/cs/{*counterStorageName}")]
		public HttpResponseMessage Disable(string counterStorageName, bool isSettingDisabled)
		{
			throw new NotImplementedException();
		}

		[HttpPost]
		[RavenRoute("admin/cs/batch-toggle-disable")]
		public HttpResponseMessage ToggleDisable(bool isSettingDisabled)
		{
			throw new NotImplementedException();
		}

        private bool IsCounterStorageNameExists(string counterStorageName)
        {
			var docKey = Constants.Counter.Prefix + counterStorageName;
            var database = Database.Documents.Get(docKey, null);
            return database != null;
        }
    }
}