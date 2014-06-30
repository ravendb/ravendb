//-----------------------------------------------------------------------
// <copyright file="TransactionCommit.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Extensions;
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
		    var resourceManagerIdStr = context.Request.QueryString["resourceManagerId"];

		    Guid resourceManagerId;
		    if (Guid.TryParse(resourceManagerIdStr, out resourceManagerId))
		    {
		        var recoveryInformation = context.Request.InputStream.ReadData();
                if (recoveryInformation == null || recoveryInformation.Length ==0)
                    throw new InvalidOperationException("Recovery information is mandatory if resourceManagerId is specified");

		        Database.PrepareTransaction(txId, resourceManagerId, recoveryInformation);
		    }
		    else
		    {
		        Database.PrepareTransaction(txId);
		    }
			context.WriteJson(new { Prepared = txId });
		}
	}
}