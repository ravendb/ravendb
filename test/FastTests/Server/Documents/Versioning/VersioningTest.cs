using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Versioning;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Documents.Versioning
{
    public class VersioningHelper
    {
        public static async Task SetupVersioning(Raven.Server.ServerWide.ServerStore serverStore, string database, bool purgeOnDelete = true, long maxRevisions = 123)
        {
            var versioningDoc = new VersioningConfiguration
            {
                Default = new VersioningConfigurationCollection
                {
                    Active = true,
                    MaxRevisions = 5,
                },
                Collections = new Dictionary<string, VersioningConfigurationCollection>
                {
                    ["Users"] = new VersioningConfigurationCollection
                    {
                        Active = true,
                        PurgeOnDelete = purgeOnDelete,
                        MaxRevisions = maxRevisions
                    },
                    ["Comments"] = new VersioningConfigurationCollection
                    {
                        Active = false,
                    },
                    ["Products"] = new VersioningConfigurationCollection
                    {

                        Active = false,
                    },
                }
            };

            await SetupVersioning(serverStore, database, versioningDoc);
        }

        public static async Task SetupVersioning(Raven.Server.ServerWide.ServerStore serverStore, string database, VersioningConfiguration configuration)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                await serverStore.ModifyDatabaseVersioningBundle(context, database,
                    EntityToBlittable.ConvertEntityToBlittable(configuration, DocumentConventions.Default, context));
            }
        }
    }
}