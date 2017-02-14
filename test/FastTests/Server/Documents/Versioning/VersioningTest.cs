using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Server.Documents.Versioning;

namespace FastTests.Server.Documents.Versioning
{
    public class VersioningHelper
    {
        public static async Task SetupVersioning(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new VersioningConfiguration
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
                            PurgeOnDelete = true,
                            MaxRevisions = 123
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
                }, Constants.Documents.Versioning.ConfigurationKey);

                await session.SaveChangesAsync();
            }
        }
    }
}