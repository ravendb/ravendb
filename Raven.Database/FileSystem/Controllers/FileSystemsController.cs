using System.Net;
using Raven.Abstractions.Data;
// -----------------------------------------------------------------------
//  <copyright file="FileSystemsController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.FileSystem;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.FileSystem.Controllers
{
    public class FileSystemsController : RavenDbApiController
    {
        [HttpGet]
        [RavenRoute("fs")]
        public HttpResponseMessage FileSystems(bool getAdditionalData = false)
        {
            if (EnsureSystemDatabase() == false)
                return
                    GetMessageWithString(
                        "The request '" + InnerRequest.RequestUri.AbsoluteUri + "' can only be issued on the system database",
                        HttpStatusCode.BadRequest);

            // This method is NOT secured, and anyone can access it.
            // Because of that, we need to provide explicit security here.

            // Anonymous Access - All / Get / Admin
            // Show all file systems

            // Anonymous Access - None
            // Show only the file system that you have access to (read / read-write / admin)

            // If admin, show all file systems

            var fileSystemsDocument = GetFileSystemsDocuments();
            var fileSystemsData = GetFileSystemsData(fileSystemsDocument);
            var fileSystemsNames = fileSystemsData.Select(fileSystemObject => fileSystemObject.Name).ToArray();

            List<string> approvedFileSystems = null;
            if (SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
            {
                var authorizer = (MixedModeRequestAuthorizer)ControllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
                HttpResponseMessage authMsg;
                if (authorizer.TryAuthorize(this, out authMsg) == false)
                    return authMsg;

                var user = authorizer.GetUser(this);
                if (user == null)
                    return authMsg;

                if (user.IsAdministrator(SystemConfiguration.AnonymousUserAccessMode) == false)
                {
                    approvedFileSystems = authorizer.GetApprovedResources(user, this, fileSystemsNames);
                }

                fileSystemsData.ForEach(x =>
                {
                    var principalWithDatabaseAccess = user as PrincipalWithDatabaseAccess;
                    if (principalWithDatabaseAccess != null)
                    {
                        var isAdminGlobal = principalWithDatabaseAccess.IsAdministrator(SystemConfiguration.AnonymousUserAccessMode);
                        x.IsAdminCurrentTenant = isAdminGlobal || principalWithDatabaseAccess.IsAdministrator(Database);
                    }
                    else
                    {
                        x.IsAdminCurrentTenant = user.IsAdministrator(x.Name);
                    }
                });
            }

            var lastDocEtag = GetLastDocEtag();
            if (MatchEtag(lastDocEtag))
                return GetEmptyMessage(HttpStatusCode.NotModified);

            if (approvedFileSystems != null)
            {
                fileSystemsData = fileSystemsData.Where(databaseData => approvedFileSystems.Contains(databaseData.Name)).ToList();
                fileSystemsNames = fileSystemsNames.Where(databaseName => approvedFileSystems.Contains(databaseName)).ToArray();
            }

            var responseMessage = getAdditionalData ? GetMessageWithObject(fileSystemsData) : GetMessageWithObject(fileSystemsNames);
            WriteHeaders(new RavenJObject(), lastDocEtag, responseMessage);
            return responseMessage.WithNoCache();
        }

        private static List<FileSystemData> GetFileSystemsData(IEnumerable<RavenJToken> fileSystems)
        {
            var fileSystemsData = fileSystems
                .Select(fileSystem =>
                {
                    var bundles = new string[] { };
                    var settings = fileSystem.Value<RavenJObject>("Settings");
                    if (settings != null)
                    {
                        var activeBundles = settings.Value<string>("Raven/ActiveBundles");
                        if (activeBundles != null)
                        {
                            bundles = activeBundles.Split(';');
                        }
                    }
                    return new FileSystemData
                    {
                        Name = fileSystem.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(Constants.FileSystem.Prefix, string.Empty),
                        Disabled = fileSystem.Value<bool>("Disabled"),
                        Bundles = bundles,
                        IsAdminCurrentTenant = true,
                    };
                }).ToList();
            return fileSystemsData;
        }

        private class FileSystemData : TenantData
        {
        }

        private RavenJArray GetFileSystemsDocuments()
        {
            var start = GetStart();
            var nextPageStart = start; // will trigger rapid pagination
            var fileSystemsDocuments = Database.Documents.GetDocumentsWithIdStartingWith(Constants.FileSystem.Prefix, null, null, start,
                int.MaxValue, CancellationToken.None, ref nextPageStart);

            return fileSystemsDocuments;
        }

        [HttpGet]
        [RavenRoute("fs/status")]
        public HttpResponseMessage Status()
        {
            string status = "ready";
            if (!RavenFileSystem.IsRemoteDifferentialCompressionInstalled)
                status = "install-rdc";

            var result = new { Status = status };

            return GetMessageWithObject(result).WithNoCache();
        }

        [HttpGet]
        [RavenRoute("fs/stats")]
        public async Task<HttpResponseMessage> Stats()
        {
            var fileSystemsDocument = GetFileSystemsDocuments();
            var fileSystemsData = GetFileSystemsData(fileSystemsDocument);
            var fileSystemsNames = fileSystemsData.Select(fileSystemObject => fileSystemObject.Name).ToArray();

            var stats = new List<FileSystemStats>();
            foreach (var fileSystemName in fileSystemsNames)
            {
                Task<RavenFileSystem> fsTask;
                if (!FileSystemsLandlord.TryGetFileSystem(fileSystemName, out fsTask)) // we only care about active file systems
                    continue;

                if (fsTask.IsCompleted == false)
                    continue; // we don't care about in process of starting file systems

                var ravenFileSystem = await fsTask;
                var fsStats = ravenFileSystem.GetFileSystemStats();
                stats.Add(fsStats);
            }

            return GetMessageWithObject(stats).WithNoCache();
        }
    }
}
