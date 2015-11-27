// -----------------------------------------------------------------------
//  <copyright file="ConfigurationSmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem
{
    internal class ConfigurationSmuggler : SmugglerBase
    {
        private readonly Regex internalConfigs = new Regex("^(sync|deleteOp|raven\\/synchronization\\/sources|conflicted|renameOp)", RegexOptions.IgnoreCase);

        public ConfigurationSmuggler(IFileSystemSmugglerSource source, IFileSystemSmugglerDestination destination, FileSystemSmugglerOptions options, FileSystemSmugglerNotifications notifications)
            : base(source, destination, options, notifications)
        {
        }

        public override async Task SmuggleAsync(FileSystemSmugglerOperationState state, CancellationToken cancellationToken)
        {
            var written = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);

            Notifications.ShowProgress("Exporting Configurations");

            using (var writer = Destination.WriteConfigurations())
            {
                while (true)
                {
                    bool hasConfigs = false;

                    foreach (var configItem in await Source.GetConfigurations(written, Options.BatchSize).ConfigureAwait(false))
                    {
                        if (internalConfigs.IsMatch(configItem.Key))
                            continue;

                        if (Options.StripReplicationInformation)
                        {
                            if (configItem.Key.Equals(SynchronizationConstants.RavenSynchronizationVersionHiLo, StringComparison.OrdinalIgnoreCase))
                                continue;
                        }

                        hasConfigs = true;

                        var config = configItem.Value;

                        if (string.Equals(configItem.Key, SynchronizationConstants.RavenSynchronizationDestinations, StringComparison.OrdinalIgnoreCase))
                        {
                            config = DisableSynchronizationDestinations(config);
                        }

                        await writer.WriteConfigurationAsync(configItem.Key, config).ConfigureAwait(false);
                        
                        written++;

                        if (written % 100 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                        {
                            Notifications.ShowProgress("Exported {0} configurations. ", written);
                            lastReport = SystemTime.UtcNow;
                        }
                    }

                    if (hasConfigs == false)
                        break;
                }
            }

            Notifications.ShowProgress("Done with exporting configurations");
        }

        private static RavenJObject DisableSynchronizationDestinations(RavenJObject config)
        {
            var destinationsConfig = config.JsonDeserialization<SynchronizationDestinationsConfig>();

            foreach (var destination in destinationsConfig.Destinations)
            {
                destination.Enabled = false;
            }

            return RavenJObject.FromObject(destinationsConfig);
        }
    }
}