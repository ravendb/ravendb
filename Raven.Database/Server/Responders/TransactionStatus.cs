using System;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
	public class TransactionStatus : RequestResponder
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