using System;
using System.Collections.Generic;
using Raven.Server.Config.Attributes;
using Sparrow.Json.Parsing;

namespace Raven.Server.Config
{
    public abstract class ConfigurationEntryValue : IDynamicJson
    {
        protected ConfigurationEntryValue(ConfigurationEntryMetadata metadata)
        {
            Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        }

        public ConfigurationEntryMetadata Metadata { get; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Metadata)] = Metadata.ToJson()
            };
        }
    }

    public class ConfigurationEntryServerValue : ConfigurationEntryValue
    {
        public ConfigurationEntryServerValue(RavenConfiguration configuration, ConfigurationEntryMetadata metadata, RavenServer.AuthenticationStatus authenticationStatus)
            : base(metadata)
        {
            ServerValues = new Dictionary<string, ConfigurationEntrySingleValue>();
            foreach (var key in Metadata.Keys)
            {
                var value = configuration.GetServerWideSetting(key);
                if (value == null)
                    continue;

                var hasAccess = authenticationStatus == RavenServer.AuthenticationStatus.ClusterAdmin;
                ServerValues[key] = new ConfigurationEntrySingleValue
                {
                    Value = hasAccess && Metadata.IsSecured == false ? value : null,
                    HasAccess = hasAccess
                };
            }
        }

        public Dictionary<string, ConfigurationEntrySingleValue> ServerValues { get; }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();

            var serverValues = new DynamicJsonValue();
            foreach (var kvp in ServerValues)
                serverValues[kvp.Key] = kvp.Value.ToJson();

            djv[nameof(ServerValues)] = serverValues;

            return djv;
        }
    }

    public class ConfigurationEntryDatabaseValue : ConfigurationEntryServerValue
    {
        public ConfigurationEntryDatabaseValue(RavenConfiguration configuration, ConfigurationEntryMetadata metadata, RavenServer.AuthenticationStatus authenticationStatus)
            : base(configuration, metadata, authenticationStatus)
        {
            if (Metadata.Scope == ConfigurationEntryScope.ServerWideOnly)
                return;

            DatabaseValues = new Dictionary<string, ConfigurationEntrySingleValue>();
            foreach (var key in Metadata.Keys)
            {
                var value = configuration.GetSetting(key);
                if (value == null)
                    continue;

                DatabaseValues[key] = new ConfigurationEntrySingleValue
                {
                    Value = Metadata.IsSecured == false ? value : null,
                    HasAccess = true
                };
            }
        }

        public Dictionary<string, ConfigurationEntrySingleValue> DatabaseValues { get; }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();

            DynamicJsonValue databaseValues = null;
            if (DatabaseValues != null)
            {
                databaseValues = new DynamicJsonValue();
                foreach (var kvp in DatabaseValues)
                    databaseValues[kvp.Key] = kvp.Value.ToJson();
            }

            djv[nameof(DatabaseValues)] = databaseValues;

            return djv;
        }
    }

    public class ConfigurationEntrySingleValue : IDynamicJson
    {
        public string Value { get; set; }
        public bool HasAccess { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Value)] = Value,
                [nameof(HasAccess)] = HasAccess
            };
        }
    }
}
