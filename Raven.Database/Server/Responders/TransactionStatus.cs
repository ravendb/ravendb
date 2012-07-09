using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class TransactionStatus : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/transaction/status$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var txId = context.Request.QueryString["tx"];
			context.WriteJson(new {Exists = Database.HasTransaction(new Guid(txId))});
		}
	}
}