using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Server.Versioning;
using Raven.Server.Documents.Versioning;
using Sparrow.Json;

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

        private static async Task SetupVersioning(Raven.Server.ServerWide.ServerStore serverStore, string database, VersioningConfiguration configuration)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                await serverStore.ModifyDatabaseVersioning(context, database,
                    EntityToBlittable.ConvertEntityToBlittable(configuration, DocumentConventions.Default, context));
            }
        }
    }
}