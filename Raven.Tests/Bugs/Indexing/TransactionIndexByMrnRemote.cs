using System.ComponentModel.Composition.Hosting;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
    public class TransactionIndexByMrnRemote : RemoteClientTest
    {
        [Fact]
        public void CanCreateIndex()
        {
            using(GetNewServer())
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {
                IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(Transaction_ByMrn))), store);
            }
        }
    }
}