using FastTests;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Bugs.LiveProjections
{
    public class CanLoadMultipleItems : RavenTestBase
    {
        private class Person
        {
            public string Name { get; set; }
            public string[] Children { get; set; }
        }

        private class ParentAndChildrenNames : AbstractIndexCreationTask<Person>
        {
            public ParentAndChildrenNames()
            {
                Map = people => from person in people
                                where person.Children.Length > 0
                                select new { person.Name };
            }

            public class ParentAndChildrenNamesTransformer : AbstractTransformerCreationTask<Person>
            {
                public ParentAndChildrenNamesTransformer()
                {
                    TransformResults = people =>
                    from person in people
                    let children = LoadDocument<Person>(person.Children)
                    select new { person.Name, ChildrenNames = children.Select(x => x.Name) };
                }
            }
        }

        [Fact]
        public void CanLoadMultipleItemsInTransformResults()
        {
            using (var store = GetDocumentStore())
            {
                new ParentAndChildrenNames().Execute(((IDocumentStore) store));	
                new ParentAndChildrenNames.ParentAndChildrenNamesTransformer().Execute((IDocumentStore)store);

                using(var s = store.OpenSession())
                {
                    s.Store(new Person
                                {
                                    Name = "Arava"
                                });
                    s.Store(new Person
                                {
                                    Name = "Oscar"
                                });
                    s.Store(new Person
                                {
                                    Name = "Oren",
                                    Children = new string[] { "people/1" , "people/2"}
                                });
                    s.SaveChanges();

                    var results = s.Query<dynamic, ParentAndChildrenNames>().Customize(x => x.WaitForNonStaleResults())
                    .TransformWith<ParentAndChildrenNames.ParentAndChildrenNamesTransformer, dynamic>()
                    .ToArray();

                    Assert.Equal(1, results.Length);

                    Assert.Equal("Oren", (string)results[0].Name);
                    Assert.Equal("Arava", (string)results[0].ChildrenNames[0]);
                    Assert.Equal("Oscar", (string)results[0].ChildrenNames[1]);

                }
            }
        }
    }
}
