using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Database.Server.Security.Windows;

namespace Raven.Database.Server.Security.Triggers
{
	class WindowsAuthPutTrigger : AbstractPutTrigger
	{
		public override void AfterPut(string key, Raven.Json.Linq.RavenJObject document, Raven.Json.Linq.RavenJObject metadata, Etag etag, Raven.Abstractions.Data.TransactionInformation transactionInformation)
		{
			if (key == "Raven/Authorization/WindowsSettings")
				WindowsRequestAuthorizer.InvokeWindowsSettingsChanged();
			base.AfterPut(key, document, metadata, etag, transactionInformation);
		}
	}
}