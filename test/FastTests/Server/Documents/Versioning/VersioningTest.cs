using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Server.Versioning;
using Sparrow.Json;

namespace FastTests.Server.Documents.Versioning
{
    public class VersioningHelper
    {
        public static async Task<long> SetupVersioning(Raven.Server.ServerWide.ServerStore serverStore, string database, bool purgeOnDelete = true, long maxRevisions = 123)
        {
            var versioningDoc = new VersioningConfiguration
            {
                Default = new VersioningConfigurationCollection
                {
                    Active = true,
                    MinimumRevisionsToKeep = 5,
                },
                Collections = new Dictionary<string, VersioningConfigurationCollection>
                {
                    ["Users"] = new VersioningConfigurationCollection
                    {
                        Active = true,
                        PurgeOnDelete = purgeOnDelete,
                        MinimumRevisionsToKeep = maxRevisions
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

            return await SetupVersioning(serverStore, database, versioningDoc);
        }

        private static async Task<long> SetupVersioning(Raven.Server.ServerWide.ServerStore serverStore, string database, VersioningConfiguration configuration)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configurationJson = EntityToBlittable.ConvertEntityToBlittable(configuration, DocumentConventions.Default, context);
                (long etag, _) =  await serverStore.ModifyDatabaseVersioning(context, database, configurationJson);
                return etag;
            }
        }
    }
}