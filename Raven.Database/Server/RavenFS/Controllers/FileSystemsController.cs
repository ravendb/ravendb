// -----------------------------------------------------------------------
//  <copyright file="FileSystemsController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Database.Extensions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Json.Linq;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.Server.RavenFS.Controllers
{
    public class FileSystemsController : RavenDbApiController
    {
        [HttpGet]
        [Route("fs/names")]
        public HttpResponseMessage Names()
        {
            var names = GetFileSystemNames();
            return this.GetMessageWithObject(names)
                       .WithNoCache();
        }

        [HttpGet]
        [Route("fs/stats")]
        public async Task<HttpResponseMessage> Stats()
        {
            var stats = new List<FileSystemStats>();

            string[] fileSystemNames = GetFileSystemNames();

            foreach (var fileSystemName in fileSystemNames)
            {
                Task<RavenFileSystem> fsTask;
                if (!FileSystemsLandlord.TryGetFileSystem(fileSystemName, out fsTask)) // we only care about active file systems
                    continue;

                if(fsTask.IsCompleted == false)
                    continue; // we don't care about in process of starting file systems

                var ravenFileSystem = await fsTask;
                var fsStats = ravenFileSystem.GetFileSystemStats();
                stats.Add(fsStats);
            }

            return this.GetMessageWithObject(stats)
                       .WithNoCache();
        }


        private string[] GetFileSystemNames()
        {
            var start = GetStart();
            var nextPageStart = start; // will trigger rapid pagination

            var fileSystems = Database.Documents.GetDocumentsWithIdStartingWith("Raven/FileSystems/", null, null, start, GetPageSize(Database.Configuration.MaxPageSize), CancellationToken.None, ref nextPageStart);

            var fileSystemNames = fileSystems
                                    .Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/FileSystems/", string.Empty))
                                    .ToArray();

            List<string> approvedFileSystems = null;

            if (DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
            {
                var user = User;
                if (user == null)
                    return null;

                if (user.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode) == false)
                {
                    var authorizer = (MixedModeRequestAuthorizer)this.ControllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

                    approvedFileSystems = authorizer.GetApprovedResources(user, this, fileSystemNames);
                }
            }

            if (approvedFileSystems != null)
                fileSystemNames = fileSystemNames.Where(s => approvedFileSystems.Contains(s)).ToArray();

            return fileSystemNames;
        }
    }
}