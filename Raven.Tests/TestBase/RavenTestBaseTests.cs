using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.TestBase
{
    public class RavenTestBaseTests : RavenTestBase
    {
        [Fact]
        public void GivenACollectionOfSeedData_NewDocumentStore_StoresTheSeedData()
        {
            // Arrange.
            var seedData = new IEnumerable[]
            {
                FakeModelHelpers.CreateFakeCats(),
                FakeModelHelpers.CreateFakeDogs(10)
            };
            
            // Act.
            var documentStore = NewDocumentStore(seedData: seedData);
            WaitForIndexing(documentStore);

            // Assert.
            IList<Cat> cats;
            IList<Dog> dogs;
            using (var session = documentStore.OpenSession())
            {
                cats = session.Query<Cat>().ToList();
                dogs = session.Query<Dog>().ToList();
            }

            Assert.Equal(20, cats.Count);
            Assert.Equal(10, dogs.Count);
        }

        [Fact]
        public void GivenAnIndex_NewDocumentStore_StoresTheIndex()
        {
            // Arrange.
            var indexes = new[] {new Animal_Search()};

            // Act.
            var documentStore = NewDocumentStore(indexes: indexes);

            // Assert.
            // Two indexes:
            // 1 - Default : RavenDocumentsByEntityName
            // 2 - Animal/Search
            Assert.Equal(2, documentStore.DatabaseCommands.GetStatistics().CountOfIndexes);
        }
    }
}
