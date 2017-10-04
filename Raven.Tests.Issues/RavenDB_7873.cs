using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_7873 : RavenTest
    {
        [Fact]
        public void QueryingTermsWithDashesShouldWork()
        {
            using (var store = NewDocumentStore())
            {
                store.ExecuteIndex(new DogsByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog { Name = "-woof woof", Age = 5 });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    var res = session.Query<Dog>().Where(m => m.Name.StartsWith("-woof ")).ToList();
                    Assert.Equal(res.Count, 1);
                }
            }
        }

        public class DogsByName : AbstractIndexCreationTask<Dog>
        {
            public DogsByName()
            {
                Map = dogs => from dog in dogs select new { dog.Name };
            }
        }

        public class Dog
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }
    }

    
}