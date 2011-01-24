//-----------------------------------------------------------------------
// <copyright file="TransactionPromote.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
	public class TransactionPromote : RequestResponder
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
			context.WriteData(Database.PromoteTransaction(fromTxId), new JObject(), Guid.NewGuid());
		}
	}
}
