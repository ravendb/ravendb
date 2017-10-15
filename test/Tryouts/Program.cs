using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using System.Collections.Generic;
using System.Linq;

namespace RavenDB4RCTests
{
    class TestLargeIndex
    {
        public static void Main()
        {
            var documentStore = new DocumentStore
            {
                Urls = new[] { "http://localhost.fiddler:8080" },
                Database = "large-index-test",
            };

            documentStore.Initialize();

          
        }

    }
}
