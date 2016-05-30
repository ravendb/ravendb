using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Server.Documents.Expiration;

namespace FastTests.Server.Documents.Expiration
{
    public class ExpirationTest : RavenTestBase
    {
        protected async Task SetupExpiration(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new ExpirationConfiguration
                {
                    Active = true,
                    DeleteFrequencySeconds = 1,
                }, Constants.Expiration.RavenExpirationConfiguration);

                await session.SaveChangesAsync();
            }
        }
    }
}