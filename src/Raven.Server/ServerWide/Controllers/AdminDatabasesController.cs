using System.IO;
using System.Net;
using System.Net.Http;
using Microsoft.AspNet.Mvc;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Server.Utils.Web;
using JsonTextReader = Raven.Imports.Newtonsoft.Json.JsonTextReader;

namespace Raven.Server.ServerWide.Controllers
{
    public class AdminDatabasesController : RavenController
    {
        private readonly ServerStore _serverStore;

        public AdminDatabasesController(ServerStore serverStore)
        {
            _serverStore = serverStore;
        }

        [HttpGet]
        [Route("admin/databases")]
        public ActionResult Get(string id)
        {
            var db = _serverStore.Read("db/" + id);
            if (db == null)
                return new HttpNotFoundResult();

            return new JsonActionResult(db);
        }

        [HttpPut]
        [Route("admin/databases")]
        public ActionResult Put(string id)
        {
            var val = RavenJObject.Load(new JsonTextReader(new StreamReader(Request.Body)));

            _serverStore.Write("db/" + id, val);
            return new HttpOkResult();
        }
    }
}