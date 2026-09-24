using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_22083 : RavenTestBase
    {
        public RavenDB_22083(ITestOutputHelper output) : base(output)
        {
        }

        private class Animal
        {
            public string Name { get; set; }
        }

        private class Dog : Animal
        {
        }

        private class Cat : Animal
        {
        }

        private class MultiMapIndexWithAddMapForAll : AbstractMultiMapIndexCreationTask
        {
            public MultiMapIndexWithAddMapForAll()
            {
                AddMapForAll<Animal>(animals => from animal in animals select new { animal.Name });
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void MultiMap_Index_With_AddMapForAll_Creates_Definition()
        {
            var definition = new MultiMapIndexWithAddMapForAll().CreateIndexDefinition();

            Assert.Equal(3, definition.Maps.Count);
        }
    }
}
