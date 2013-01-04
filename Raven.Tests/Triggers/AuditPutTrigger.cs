//-----------------------------------------------------------------------
// <copyright file="AuditPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Tests.Triggers
{
	public class AuditPutTrigger : AbstractPutTrigger
	{
		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			return VetoResult.Allowed;
		}

		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			document["created_at"] = new RavenJValue(new DateTime(2010, 2, 13, 18, 26, 48, 506, DateTimeKind.Utc));
		}
	}
}
