//-----------------------------------------------------------------------
// <copyright file="TransactionCommit.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class TransactionCommit : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/transaction/commit$"; }
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
