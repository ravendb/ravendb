using Raven.Client.Documents;
using Tests.Infrastructure.Utils;

namespace Tests.Infrastructure.Extensions;

public static class DocumentStoreSessionTestingExtensions
{
    public static SessionTester ForSessionTesting(this IDocumentStore store)
    {
        return new SessionTester(store);
    }
}
