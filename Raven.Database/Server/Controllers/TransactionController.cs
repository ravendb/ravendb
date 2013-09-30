using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class TransactionController : RavenApiController
	{
		[HttpPost("transaction/rollback")]
		public HttpResponseMessage Rollback()
		{
			var txId = GetQueryStringValue("tx");
			Database.Rollback(txId);
			return GetMessageWithObject(new { Rollbacked = txId });
		}

		[HttpGet("transaction/status")]
		public HttpResponseMessage Status()
		{
			var txId = GetQueryStringValue("tx");
			return GetMessageWithObject(new { Exists = Database.HasTransaction(txId) });
		}

		[HttpPost("transaction/prepare")]
		public HttpResponseMessage Prepare()
		{
			var txId = GetQueryStringValue("tx");

			Database.PrepareTransaction(txId);
			return GetMessageWithObject(new { Prepared = txId });
		}

		[HttpPost("transaction/commit")]
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
