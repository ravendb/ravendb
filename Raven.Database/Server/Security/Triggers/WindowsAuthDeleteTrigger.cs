using Raven.Database.Plugins;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;

namespace Raven.Database.Server.Security.Triggers
{
    class WindowsAuthDeleteTrigger : AbstractDeleteTrigger
    {
        public override void AfterDelete(string key, Raven.Abstractions.Data.TransactionInformation transactionInformation,RavenJObject metadata)
        {
            if (key == "Raven/Authorization/WindowsSettings")
                WindowsRequestAuthorizer.InvokeWindowsSettingsChanged();
            base.AfterDelete(key, transactionInformation);
        }
    }
}
