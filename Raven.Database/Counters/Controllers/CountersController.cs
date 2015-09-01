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
	public class CountersController : RavenDbApiController
	{
		[RavenRoute("cs")]
		[HttpGet]
		public HttpResponseMessage Counters(bool getAdditionalData = false)
		{
			return Resources(Constants.Counter.Prefix, GetCounterStoragesData, getAdditionalData);
		}

		private class CounterStorageData : TenantData
		{
		}

		private static List<CounterStorageData> GetCounterStoragesData(IEnumerable<RavenJToken> counterStorages)
		{
			return counterStorages
				.Select(counterStorage =>
				{
					var bundles = new string[] {};
					var settings = counterStorage.Value<RavenJObject>("Settings");
					if (settings != null)
					{
						var activeBundles = settings.Value<string>("Raven/ActiveBundles");
						if (activeBundles != null)
						{
							bundles = activeBundles.Split(';');
						}
					}
					return new CounterStorageData
					{
						Name = counterStorage.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(Constants.Counter.Prefix, string.Empty),
						Disabled = counterStorage.Value<bool>("Disabled"),
						Bundles = bundles,
						IsAdminCurrentTenant = true,
					};
				}).ToList();
		}
	}
}