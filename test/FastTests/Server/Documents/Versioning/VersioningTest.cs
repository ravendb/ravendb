using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Server.Documents.Versioning;

namespace FastTests.Server.Documents.Versioning
{
    public class VersioningTest : RavenTestBase
    {
        protected async Task SetupVersioning(DocumentStore store)
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
                            Active = false,
                        },
                        ["Comments"] = new VersioningConfigurationCollection
                        {
                            Active = false,
                        },
                    }
                }, Constants.Versioning.RavenVersioningConfiguration);

                await session.SaveChangesAsync();
            }
        }
    }
}