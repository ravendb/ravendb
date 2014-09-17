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
            get { return "^/admin/transactions/rollback"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }

	    public override void RespondToAdmin(IHttpContext context)
	    {
			var txId = context.Request.QueryString["tx"];
		    if (string.IsNullOrEmpty(txId) == false)
		    {
			    Database.Rollback(txId);

				context.WriteJson(new { RolledBackTransactionsAmount = 1});
			    return;
		    }

		    var rollback = context.Request.QueryString["rollback"];
		    if ("all".Equals(rollback, StringComparison.OrdinalIgnoreCase) == false)
		    {
				context.SetStatusToBadRequest(); 
				context.WriteJson(new
				{
					Error = "The query string should contain ?id=tx-id or ?rollback=all"
				});
				return;
		    }

			var transactions = Database.TransactionalStorage.GetTransactionContextsData();
			foreach (var transactionContextData in transactions)
			{
				Database.Rollback(transactionContextData.Id);
			}
			context.WriteJson(new { RolledBackTransactionsAmount = transactions.Count });
	    }
    }
}