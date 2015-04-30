using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
	public class CountersController : RavenDbApiController
	{
		[RavenRoute("cs/counterStorageNames")]
		[HttpGet]
		public HttpResponseMessage GetCounterStorageNames()
		{
			var names = GetCounterStorages();
			return GetMessageWithObject(names);
		}

		private string[] GetCounterStorages()
		{
			var start = GetStart();
			var nextPageStart = start; // will trigger rapid pagination
			var counterStorages = DatabasesLandlord.SystemDatabase.Documents.GetDocumentsWithIdStartingWith("Raven/Counters/", null, null, start, GetPageSize(DatabasesLandlord.SystemDatabase.Configuration.MaxPageSize), CancellationToken.None, ref nextPageStart);
			var counterStoragesNames = counterStorages
									.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(Constants.Counter.Prefix, string.Empty))
									.ToArray();
			return counterStoragesNames;
		}
	}
}