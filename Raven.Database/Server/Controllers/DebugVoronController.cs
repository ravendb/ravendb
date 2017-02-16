// -----------------------------------------------------------------------
//  <copyright file="DebugVoronController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Storage.Voron;
using Voron;
using Voron.Debugging;

namespace Raven.Database.Server.Controllers
{
    public class DebugVoronController : BaseAdminDatabaseApiController
    {
        [HttpGet]
        [RavenRoute("admin/voron/tree")]
        [RavenRoute("databases/{databaseName}/admin/voron/tree")]
        public HttpResponseMessage DumpTree(string name)
        {
            var transactionalStorage = Database.TransactionalStorage as Raven.Storage.Voron.TransactionalStorage;
            if (transactionalStorage == null)
            {
                return GetMessageWithObject(new
                {
                    Error = "The database storage is not Voron"
                }, HttpStatusCode.BadRequest);
            }
            using (var tx = transactionalStorage.Environment.NewTransaction(TransactionFlags.Read))
            {
                var readTree = tx.ReadTree(name);
                if (readTree == null)
                {
                    return GetMessageWithObject(new
                    {
                        Error = "The database storage does not contains a tree named: " + name
                    }, HttpStatusCode.NotFound);
                }
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new PushStreamContent((stream, content, arg3) =>
                {
                    using (stream)
                    using (var tx = transactionalStorage.Environment.NewTransaction(TransactionFlags.Read))
                    {
                        var readTree = tx.ReadTree(name);

                        DebugStuff.DumpTreeToStream(readTree, stream);
                    }
                })
                {
                    Headers = { ContentType = MediaTypeHeaderValue.Parse("text/html")}
                }
            };
        }

        [HttpGet]
        [RavenRoute("admin/voron/scratch-buffer-pool-info")]
        [RavenRoute("databases/{databaseName}/admin/voron/scratch-buffer-pool-info")]
        public HttpResponseMessage ScratchBufferPoolInfo()
        {
            var transactionalStorage = Database.TransactionalStorage as TransactionalStorage;
            if (transactionalStorage == null)
            {
                return GetMessageWithObject(new
                {
                    Error = "The database storage is not Voron"
                }, HttpStatusCode.BadRequest);
            }

            var info = transactionalStorage.GetStorageStats();
            return GetMessageWithObject(info);
        }
    }
}
