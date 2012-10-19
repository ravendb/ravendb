using Raven.Database.Plugins;
using Raven.Database.Server.Security.Windows;

namespace Raven.Database.Server.Security.Triggers
{
	class WindowsAuthDeleteTrigger : AbstractDeleteTrigger
	{
		public override void AfterDelete(string key, Raven.Abstractions.Data.TransactionInformation transactionInformation)
		{
			if (key == "Raven/Authorization/WindowsSettings")
				WindowsRequestAuthorizer.InvokeWindowsSettingsChanged();
			base.AfterDelete(key, transactionInformation);
		}
	}
}