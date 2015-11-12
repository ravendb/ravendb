using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
    public class CountersController : BaseDatabaseApiController
    {
        [RavenRoute("cs")]
        [HttpGet]
        public HttpResponseMessage Counters(int skip, int take, bool getAdditionalData = false)
        {
            return Resources(Constants.Counter.Prefix, storages => GetCounterStoragesData(storages,skip,take), getAdditionalData);
        }

	    [RavenRoute("cs/exists")]
	    [HttpGet]
	    public HttpResponseMessage Exists(string storageName)
	    {
			HttpResponseMessage message = null;
			Resource.TransactionalStorage.Batch(accessor =>
			{
				message = GetMessageWithObject(new
				{
					Exists = accessor.Documents.DocumentByKey(Constants.Counter.Prefix + storageName) != null
				});
			});

		    return message;
	    }

        private class CounterStorageData : TenantData
        {
        }

        private static List<CounterStorageData> GetCounterStoragesData(IEnumerable<RavenJToken> counterStorages, int skip, int take)
        {
            return counterStorages
                .Select(counterStorage =>
                {
                    var bundles = new string[] {};
                    var settings = counterStorage.Value<RavenJObject>("Settings");
	                var activeBundles = settings?.Value<string>("Raven/ActiveBundles");
	                if (activeBundles != null)
	                {
		                bundles = activeBundles.Split(';');
	                }
	                return new CounterStorageData
                    {
                        Name = counterStorage.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(Constants.Counter.Prefix, string.Empty),
                        Disabled = counterStorage.Value<bool>("Disabled"),
                        Bundles = bundles,
                        IsAdminCurrentTenant = true,
                    };
                }).Skip(skip).Take(take).ToList();
        }
    }
}
