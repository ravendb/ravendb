using System;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
    public class TransactionRollback : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/transaction/rollback$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }

        public override void Respond(IHttpContext context)
        {
            var txId = context.Request.QueryString["tx"];
            Database.Rollback(new Guid(txId));
            context.WriteJson(new { Rollbacked = txId });
        }
    }
}
