using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12490 : RavenTestBase
    {
        private class EntityWithArray
        {
            public string Name;
            public List<int> Numbers;
            public string[] Strings;
        }

        [Fact]
        public void ContainsAllWorks()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new EntityWithArray
                        {
                            Name = "johnny" + i,
                            Numbers = Enumerable.Range(0, i + 1).Select(x => x).ToList(),
                            Strings = Enumerable.Range(0, i + 1).Select(x => x.ToString()).ToArray()
                        });
                    }
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var docQuery1 = session.Query<EntityWithArray>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Strings.ContainsAll(
                          new string[] { "1", "2", "3" }));
                    var res = docQuery1.ToList();
                    Assert.Equal(7, res.Count());

                    var docQuery2 = session.Query<EntityWithArray>().Where(x => x.Strings.ContainsAll(
                        new string[] { "1", "2", "555" }));

                    Assert.Equal(0, docQuery2.Count());


                    var docQuery3 = session.Query<EntityWithArray>().Where(x => x.Strings.ContainsAny(
                        new string[] { "1", "2", "555" }));

                    Assert.Equal(9, docQuery3.Count());

                    var docQuery4 = session.Query<EntityWithArray>().Where(x => x.Strings.ContainsAny(
                        new string[] { "333", "222", "555" }));

                    Assert.Equal(0, docQuery4.Count());


                }

                using (var session = store.OpenSession())
                {
                    var docQuery1 = session.Advanced.DocumentQuery<EntityWithArray>().ContainsAll(x => x.Strings,
                        new string[] { "1", "2", "3" });
                    Assert.Equal(7, docQuery1.Count());

                    var docQuery2 = session.Advanced.DocumentQuery<EntityWithArray>().ContainsAll(x => x.Strings,
                        new string[] { "1", "2", "555" });

                    Assert.Equal(0, docQuery2.Count());


                    var docQuery3 = session.Advanced.DocumentQuery<EntityWithArray>().ContainsAny(x => x.Strings,
                        new string[] { "1", "2", "555" });

                    Assert.Equal(9, docQuery3.Count());

                    var docQuery4 = session.Advanced.DocumentQuery<EntityWithArray>().ContainsAny(x => x.Strings,
                        new string[] { "333", "222", "555" });

                    Assert.Equal(0, docQuery4.Count());
                }


            }
        }
    }

}
