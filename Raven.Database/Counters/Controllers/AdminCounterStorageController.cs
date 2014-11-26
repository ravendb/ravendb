// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Counters;
using Raven.Database.Extensions;
using Raven.Database.Server.Controllers.Admin;
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
        [Route("counterstorage/admin/{*id}")]
        public async Task<HttpResponseMessage> Put(string id)
        {
            var docKey = "Raven/Counters/" + id;

			bool isCounterStorageUpdate = CheckQueryStringParameterResult("update");
			if (IsCounterStorageNameExists(id) && !isCounterStorageUpdate)
            {
				return GetMessageWithString(string.Format("Counter Storage {0} already exists!", id), HttpStatusCode.Conflict);
            }

            var dbDoc = await ReadJsonObjectAsync<CountersDocument>();
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
        [Route("counterstorage/admin/{*id}")]
		public HttpResponseMessage Delete(string id)
		{
            var docKey = "Raven/Counters/" + id;
            var configuration = CountersLandlord.CreateTenantConfiguration(id);

			if (configuration == null)
				return GetEmptyMessage();

            if (!IsCounterStorageNameExists(id))
            {
                return GetMessageWithString(string.Format("Counter Storage {0} doesn't exist!", id), HttpStatusCode.NotFound);
            }

			Database.Documents.Delete(docKey, null, null);

			bool isHardDeleteNeeded = CheckQueryStringParameterResult("hard-delete");
			if (isHardDeleteNeeded)
			{
				IOExtensions.DeleteDirectory(configuration.CountersDataDirectory);
			}

			return GetEmptyMessage();
		}

        private bool IsCounterStorageNameExists(string id)
        {
            var docKey = "Raven/Counters/" + id;
            var database = Database.Documents.Get(docKey, null);
            return database != null;
        }
    }
}