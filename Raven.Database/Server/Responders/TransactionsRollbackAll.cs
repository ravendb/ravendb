using System;
using System.Linq;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders.Admin;

namespace Raven.Database.Server.Responders
{
    public class TransactionsRollbackAll : AdminResponder
    {
        public override string UrlPattern
        {
            get { return "^/admin/transactions/rollbackAll"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }

	    public override void RespondToAdmin(IHttpContext context)
	    {
			var transactions = Database.TransactionalStorage.GetPreparedTransactions();
			foreach (var transactionContextData in transactions)
			{
				Database.Rollback(transactionContextData.Id);
			}
			context.WriteJson(new { RolledBackTransactionsAmount = transactions.Count });
	    }
    }
}