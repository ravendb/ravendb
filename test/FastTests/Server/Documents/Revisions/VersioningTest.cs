using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Revisions;
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
                    Active = true,
                    MinimumRevisionsToKeep = 5,
                },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        Active = true,
                        PurgeOnDelete = purgeOnDelete,
                        MinimumRevisionsToKeep = minimumRevisionsToKeep
                    },
                    ["Comments"] = new RevisionsCollectionConfiguration
                    {
                        Active = false,
                    },
                    ["Products"] = new RevisionsCollectionConfiguration
                    {
                        Active = false,
                    },
                }
            };

            return await SetupRevisions(serverStore, database, configuration);
        }

        private static async Task<long> SetupRevisions(Raven.Server.ServerWide.ServerStore serverStore, string database, RevisionsConfiguration configuration)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configurationJson = EntityToBlittable.ConvertEntityToBlittable(configuration, DocumentConventions.Default, context);
                (long etag, _) =  await serverStore.ModifyDatabaseRevisions(context, database, configurationJson);
                return etag;
            }
        }
    }
}