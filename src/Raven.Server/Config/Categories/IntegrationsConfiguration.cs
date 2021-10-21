using System.Collections.Generic;
using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Integrations)]
    public class IntegrationsConfiguration : ConfigurationCategory
    {
        public IntegrationsConfiguration()
        {
            PostgreSql = new PostgreSqlConfiguration();
        }

        public PostgreSqlConfiguration PostgreSql { get; }

        public override void Initialize(IConfigurationRoot settings, HashSet<string> settingsNames, IConfigurationRoot serverWideSettings, HashSet<string> serverWideSettingsNames, ResourceType type, string resourceName)
        {
            base.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);
            PostgreSql.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);

            Initialized = true;
        }

        public class PostgreSqlConfiguration : ConfigurationCategory
        {
            [Description("Indicates if PostgreSQL integration is enabled or not. Default: false")]
            [DefaultValue(false)]
            [ConfigurationEntry("Integrations.PostgreSQL.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool Enabled { get; set; }

            [Description("Port on which server is listening for a PostgreSQL connections. Default: 5433")]
            [DefaultValue(5433)]
            [ConfigurationEntry("Integrations.PostgreSQL.Port", ConfigurationEntryScope.ServerWideOnly)]
            public int Port { get; set; }
        }
    }
}
