using System;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class TransactionPromote : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "/transaction/promote"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var fromTxId = new Guid(context.Request.QueryString["fromTxId"]);
			var toTxId = new Guid(context.Request.QueryString["toTxId"]);
			Database.TransactionalStorage.Batch(actions => actions.ModifyTransactionId(fromTxId, toTxId));
			context.WriteJson(new { Promoted = true });
		}
	}
}