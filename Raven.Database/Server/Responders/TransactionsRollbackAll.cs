using System;
using System.Linq;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
    public class TransactionsRollbackAll : AbstractRequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/transaction/rollbackAll"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }

        public override void Respond(IHttpContext context)
        {
            var txId = context.Request.QueryString["tx"];
            var transactions = Database.TransactionalStorage.GetTransactionContextsData();
            foreach (var transactionContextData in transactions)
            {
                Database.Rollback(transactionContextData.Id);
            }
            context.WriteJson(new { RolledBackTransactionsAmount = transactions.Count });
        }
    }
}