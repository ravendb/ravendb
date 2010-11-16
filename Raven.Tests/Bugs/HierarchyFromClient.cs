using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class HierarchyFromClient
    {
        [Fact]
        public void CanDefineHierarchicalIndexOnTheClient()
        {
            var indexDefinition = new IndexDefinition<Person>
            {
                Map = people => from p in people
                                from c in p.Hierarchy("Children")
                                select c.Name
            }.ToIndexDefinition(new DocumentConvention());

            Assert.Equal("docs.People\r\n\t.SelectMany(p => Hierarchy(p, \"Children\"), (p, c) => c.Name)", indexDefinition.Map);
        }

        [Fact]
        public void CanDefineHierarchicalIndexOnTheClient_WithLinq()
        {
            var indexDefinition = new IndexDefinition<Person>
            {
                Map = people => from p in people
                                from c in p.Hierarchy(x=>x.Children)
                                select c.Name
            }.ToIndexDefinition(new DocumentConvention());

            Assert.Equal("docs.People\r\n\t.SelectMany(p => Hierarchy(p, \"Children\"), (p, c) => c.Name)", indexDefinition.Map);
        }

        public class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Person[] Children { get; set; }
        }
    }
}