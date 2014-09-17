using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class TransactionsList : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/transaction/list"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
            context.WriteJson(new { TransactionData = Database.TransactionalStorage.GetTransactionContextsData() });
		}
	}
}