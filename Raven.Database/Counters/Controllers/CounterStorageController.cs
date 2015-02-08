using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Web.Http;

using Raven.Abstractions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
    public class CounterStorageController : RavenDbApiController
    {
		[RavenRoute("counterStorage/conterStorages")]
        [HttpGet]
        public HttpResponseMessage GetCounterStoragesNames()
        {
            var names = GetCounterStorages();
            return GetMessageWithObject(names);
        }

		[RavenRoute("counterStorage/stats")]
		[HttpGet]
		public HttpResponseMessage GetCounterStoragesStats()
		{
			//TODO: implement getting the stats about the counter storages
			return GetEmptyMessage();
		}

        private string[] GetCounterStorages()
        {
            var start = GetStart();
            var nextPageStart = start; // will trigger rapid pagination
            var counterStorages = Database.Documents.GetDocumentsWithIdStartingWith("Raven/Counters/", null, null, start, GetPageSize(Database.Configuration.MaxPageSize), CancellationToken.None, ref nextPageStart);
            var counterStoragesNames = counterStorages
                                    .Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Counters/", string.Empty))
                                    .ToArray();
            return counterStoragesNames;
        }
    }
}
