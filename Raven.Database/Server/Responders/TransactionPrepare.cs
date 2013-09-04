//-----------------------------------------------------------------------
// <copyright file="TransactionCommit.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class TransactionPrepare : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/transaction/prepare$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var txId = context.Request.QueryString["tx"];
			Database.PrepareTransaction(txId);
			context.WriteJson(new { Prepared = txId });
		}
	}
}