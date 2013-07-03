//-----------------------------------------------------------------------
// <copyright file="TransactionCommit.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
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

			var clientVersion = context.Request.Headers[Constants.RavenClientVersion];
			if (clientVersion != null && (clientVersion.StartsWith("2.0.") || clientVersion.StartsWith("1.0.")))
			{
				Database.PrepareTransaction(txId);
			}

			Database.Commit(txId);
			context.WriteJson(new {Committed = txId});
		}
	}
}