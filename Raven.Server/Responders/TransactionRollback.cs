using System;
using System.Net;

namespace Raven.Server.Responders
{
    public class TransactionRollback : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/transaction/commit"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }

        public override void Respond(HttpListenerContext context)
        {
            var txId = context.Request.QueryString["tx"];
            Database.Rollback(new Guid(txId));
            context.WriteJson(new { Rollbacked = txId });
        }
    }
}