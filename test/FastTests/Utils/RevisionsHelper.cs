using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;

namespace FastTests.Utils
{
    public class RevisionsHelper
    {
        public static async Task<long> SetupRevisionsAsync(
            IDocumentStore store,
            string database = null,
            RevisionsConfiguration configuration = null,
            Action<RevisionsConfiguration> modifyConfiguration = null)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));

            configuration ??= Default;

            modifyConfiguration?.Invoke(configuration);

            var result = await store.Maintenance.ForDatabase(database ?? store.Database).SendAsync(new ConfigureRevisionsOperation(configuration));
            return result.RaftCommandIndex ?? -1;
        }

        private static RevisionsConfiguration Default => new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 5
            },
            Collections = new Dictionary<string, RevisionsCollectionConfiguration>
            {
                ["Users"] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    PurgeOnDelete = true,
                    MinimumRevisionsToKeep = 123
                },
                ["People"] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 10
                },
                ["Comments"] = new RevisionsCollectionConfiguration
                {
                    Disabled = true
                },
                ["Products"] = new RevisionsCollectionConfiguration
                {
                    Disabled = true
                }
            }
        };
    }
}
