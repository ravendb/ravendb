using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class TransactionsList : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/debug/transactions"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
            context.WriteJson(new
            {
	            PreparedTransactions = Database.TransactionalStorage.GetPreparedTransactions()
            });
		}
	}
}