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
        public static async Task<long> SetupVersioning(Raven.Server.ServerWide.ServerStore serverStore, string database, bool purgeOnDelete = true, long minimumRevisionsToKeep = 123)
        {
            var versioningDoc = new VersioningConfiguration
            {
                Default = new VersioningCollectionConfiguration
                {
                    Active = true,
                    MinimumRevisionsToKeep = 5,
                },
                Collections = new Dictionary<string, VersioningCollectionConfiguration>
                {
                    ["Users"] = new VersioningCollectionConfiguration
                    {
                        Active = true,
                        PurgeOnDelete = purgeOnDelete,
                        MinimumRevisionsToKeep = minimumRevisionsToKeep
                    },
                    ["Comments"] = new VersioningCollectionConfiguration
                    {
                        Active = false,
                    },
                    ["Products"] = new VersioningCollectionConfiguration
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