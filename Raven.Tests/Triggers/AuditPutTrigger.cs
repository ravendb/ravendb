//-----------------------------------------------------------------------
// <copyright file="AuditPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;

namespace Raven.Tests.Triggers
{
	public class AuditPutTrigger : AbstractPutTrigger
	{
        public override VetoResult AllowPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
		{
			return VetoResult.Allowed;
		}

		public override void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
		{
			document["created_at"] = new JValue(new DateTime(2000, 1, 1,0,0,0,DateTimeKind.Utc));
		}
	}

    public class AuditAttachmentPutTrigger : AbstractAttachmentPutTrigger
    {
        public override void OnPut(string key, byte[] data, JObject metadata)
        {
            metadata["created_at"] = new JValue(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        }
    }

    public class RefuseBigAttachmentPutTrigger : AbstractAttachmentPutTrigger
    {
        public override VetoResult AllowPut(string key, byte[] data, JObject metadata)
        {
            if (data.Length > 4)
                return VetoResult.Deny("Attachment is too big");

            return VetoResult.Allowed;
        }
    }
}
