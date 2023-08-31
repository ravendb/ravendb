using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Indexes
{
    public class CreateIndexesWithCasting : RavenTestBase
    {
        public CreateIndexesWithCasting(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WillPreserverTheCasts()
        {
            var indexDefinition = new WithCasting().CreateIndexDefinition();

            var map = indexDefinition.Maps.First();

            Assert.Contains("docs.People.Select(person => new {", map);
            Assert.Contains("Id = ((long) person.Name.Length)", map);
        }

        public class WithCasting : AbstractIndexCreationTask<Person>
        {
            public WithCasting()
            {
                Map = persons => from person in persons
                                 select new { Id = (long)person.Name.Length };
            }
        }
    }
}
