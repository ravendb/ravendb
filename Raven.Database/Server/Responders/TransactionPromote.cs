//-----------------------------------------------------------------------
// <copyright file="TransactionPromote.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class TransactionPromote : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/transaction/promote$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var fromTxId = new Guid(context.Request.QueryString["fromTxId"]);
			context.WriteData(Database.PromoteTransaction(fromTxId), new RavenJObject(), Guid.NewGuid());
		}
	}
}
