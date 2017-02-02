using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class CreateIndexesWithCasting : RavenNewTestBase
    {
        [Fact]
        public void WillPreserverTheCasts()
        {
            var indexDefinition = new WithCasting
            {
                Conventions = new DocumentConvention { PrettifyGeneratedLinqExpressions = false }
            }.CreateIndexDefinition();

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
