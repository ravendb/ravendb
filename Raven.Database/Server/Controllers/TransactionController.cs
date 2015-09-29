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
			var txId = GetQueryStringValue("tx");
			Database.Rollback(txId);
			return GetMessageWithObject(new { Rollbacked = txId });
		}

		[HttpGet]
		[RavenRoute("transaction/status")]
		[RavenRoute("databases/{databaseName}/transaction/status")]
		public HttpResponseMessage Status()
		{
			var txId = GetQueryStringValue("tx");
			return GetMessageWithObject(new { Exists = Database.HasTransaction(txId) });
		}

		[HttpPost]
		[RavenRoute("transaction/prepare")]
		[RavenRoute("databases/{databaseName}/transaction/prepare")]
		public async Task<HttpResponseMessage> Prepare()
		{
			var txId = GetQueryStringValue("tx");

			var resourceManagerIdStr = GetQueryStringValue("resourceManagerId");

			Guid resourceManagerId;
			if (Guid.TryParse(resourceManagerIdStr, out resourceManagerId))
			{
				var recoveryInformation = await Request.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
				if (recoveryInformation == null || recoveryInformation.Length == 0)
					throw new InvalidOperationException("Recovery information is mandatory if resourceManagerId is specified");

				Database.PrepareTransaction(txId, resourceManagerId, recoveryInformation);
			}
			else
			{
				Database.PrepareTransaction(txId);
			}

			return GetMessageWithObject(new { Prepared = txId });
		}

		[HttpPost]
		[RavenRoute("transaction/commit")]
		[RavenRoute("databases/{databaseName}/transaction/commit")]
		public HttpResponseMessage Commit()
		{
			var txId = GetQueryStringValue("tx");

			var clientVersion = GetHeader(Constants.RavenClientVersion);
			if (clientVersion == null // v1 clients do not send this header.
				|| clientVersion.StartsWith("2.0."))
			{
				Database.PrepareTransaction(txId);
			}

			Database.Commit(txId);
			return GetMessageWithObject(new { Committed = txId });
		}
	}
}
