using System;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Json.Linq;

namespace Raven.Tests.Triggers.Bugs
{
	public class AuditTrigger : AbstractPutTrigger
	{
		public static DateTime CreatedAtDateTime = new DateTime(2011, 02, 19, 4, 8, 14, 503);

		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (AuditContext.IsInAuditContext)
				return;

			using (AuditContext.Enter())
			{
				if (metadata.Value<string>("Raven-Entity-Name") == "People")
				{
					if (metadata["CreatedByPersonId"] == null)
					{
						metadata["CreatedByPersonId"] = CurrentOperationContext.Headers.Value["CurrentUserPersonId"];
						metadata["CreatedDate"] = CreatedAtDateTime;
					}
					else
					{
						metadata["LastUpdatedPersonId"] = CurrentOperationContext.Headers.Value["CurrentUserPersonId"];
						metadata["LastUpdatedDate"] = CreatedAtDateTime;
					}
				}
			}
		}
	}
}