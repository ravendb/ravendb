//-----------------------------------------------------------------------
// <copyright file="TransactionRollback.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

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
