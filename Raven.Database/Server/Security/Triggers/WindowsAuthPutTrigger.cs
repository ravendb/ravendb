using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Database.Server.Security.Windows;

namespace Raven.Database.Server.Security.Triggers
{
	class WindowsAuthPutTrigger : AbstractPutTrigger
	{
		public override VetoResult AllowPut(string key, Raven.Json.Linq.RavenJObject document, Raven.Json.Linq.RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key == "Raven/Authorization/WindowsSettings" && Authentication.IsEnabled == false)
				return VetoResult.Deny("Cannot setup Windows Authentication without a valid commercial license.");

			return VetoResult.Allowed;
		}

		public override void AfterPut(string key, Raven.Json.Linq.RavenJObject document, Raven.Json.Linq.RavenJObject metadata, Etag etag, Raven.Abstractions.Data.TransactionInformation transactionInformation)
		{
			if (key == "Raven/Authorization/WindowsSettings")
				WindowsRequestAuthorizer.InvokeWindowsSettingsChanged();
			base.AfterPut(key, document, metadata, etag, transactionInformation);
		}
	}
}