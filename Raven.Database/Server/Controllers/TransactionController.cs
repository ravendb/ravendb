using System;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
    [RoutePrefix("")]
    public class TransactionController : BaseDatabaseApiController
    {
        [HttpPost]
        [RavenRoute("transaction/rollback")]
        [RavenRoute("databases/{databaseName}/transaction/rollback")]
        public HttpResponseMessage Rollback()
        {
            throw new NotSupportedException("DTC is not supported.");
        }

        [HttpGet]
        [RavenRoute("transaction/status")]
        [RavenRoute("databases/{databaseName}/transaction/status")]
        public HttpResponseMessage Status()
        {
            throw new NotSupportedException("DTC is not supported.");
        }

        [HttpPost]
        [RavenRoute("transaction/prepare")]
        [RavenRoute("databases/{databaseName}/transaction/prepare")]
        public async Task<HttpResponseMessage> Prepare()
        {
            throw new NotSupportedException("DTC is not supported.");
        }

        [HttpPost]
        [RavenRoute("transaction/commit")]
        [RavenRoute("databases/{databaseName}/transaction/commit")]
        public HttpResponseMessage Commit()
        {
            throw new NotSupportedException("DTC is not supported.");
        }
    }
}
