using System;
using System.Net;
using Raven.Server.Abstractions;

namespace Raven.Server.Responders
{
    public class TransactionCommit : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/transaction/commit"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }

        public override void Respond(IHttpContext context)
        {
            var txId = context.Request.QueryString["tx"];
            Database.Commit(new Guid(txId));
            context.WriteJson(new {Committed = txId});
        }
    }
}