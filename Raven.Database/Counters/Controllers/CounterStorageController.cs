using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Database.Server.Controllers;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
    public class CounterStorageController : RavenDbApiController
    {
        [Route("counterStorage/conterStorages")]
        [HttpGet]
        public HttpResponseMessage CounterStorages()
        {
            var names = GetCounterStorages();
            return GetMessageWithObject(names);
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
