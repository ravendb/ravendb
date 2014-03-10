// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Server.Controllers.Admin;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Controllers
{
    public class AdminFileSystemController : BaseAdminController
    {
        [HttpPut]
        [Route("ravenfs/admin/{*id}")]
        public async Task<HttpResponseMessage> Put(string id)
        {
            var docKey = "Raven/FileSystems/" + id;
            var dbDoc = await ReadJsonObjectAsync<DatabaseDocument>();
            FileSystemsLandlord.Protect(dbDoc);
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");

            Database.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage();
        }

        [HttpGet]
        [Route("ravenfs/admin/FileSystems")]
        public HttpResponseMessage FileSystems()
        {
            var start = GetStart();
            var nextPageStart = start; // will trigger rapid pagination

            var fileSystems = Database.GetDocumentsWithIdStartingWith("Raven/FileSystems/", null, null, start, GetPageSize(Database.Configuration.MaxPageSize), CancellationToken.None, ref nextPageStart);

            var fileSystemNames = fileSystems
                .Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/FileSystems/", string.Empty))
                .ToArray();

            return GetMessageWithObject(fileSystemNames);
        }
    }
}