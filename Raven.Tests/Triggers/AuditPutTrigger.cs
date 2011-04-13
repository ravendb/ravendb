//-----------------------------------------------------------------------
// <copyright file="AuditPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database.Plugins;
using Raven.Http;
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
			document["created_at"] = new RavenJValue(new DateTime(2000, 1, 1,0,0,0,DateTimeKind.Utc));
		}
	}
}
