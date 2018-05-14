using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace FastTests.Server.Documents.Revisions
{
    public class RevisionsHelper
    {
        public static async Task<long> SetupRevisions(Raven.Server.ServerWide.ServerStore serverStore, string database, bool purgeOnDelete = true, long minimumRevisionsToKeep = 123)
        {
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 5,
                },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = purgeOnDelete,
                        MinimumRevisionsToKeep = minimumRevisionsToKeep
                    },
                    ["Comments"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = true
                    },
                    ["Products"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = true
                    },
                }
            };

            return await SetupRevisions(serverStore, database, configuration);
        }

        private static async Task<long> SetupRevisions(Raven.Server.ServerWide.ServerStore serverStore, string database, RevisionsConfiguration configuration)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                (long etag, _) =  await serverStore.ModifyDatabaseRevisions(context, database, configurationJson);
                return etag;
            }
        }
    }
}
