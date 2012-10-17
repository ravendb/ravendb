using System;
using Raven.Database.Plugins;
using Raven.Database.Server.Security.Windows;

namespace Raven.Database.Server.Security.Triggers
{
	class WindowsAuthPutTrigger : AbstractPutTrigger
	{
		public override void AfterPut(string key, Raven.Json.Linq.RavenJObject document, Raven.Json.Linq.RavenJObject metadata, Guid etag, Raven.Abstractions.Data.TransactionInformation transactionInformation)
		{
			if (key == "Raven/Authorization/WindowsSettings")
				WindowsRequestAuthorizer.UpdateSettings();
			base.AfterPut(key, document, metadata, etag, transactionInformation);
		}
	}
}