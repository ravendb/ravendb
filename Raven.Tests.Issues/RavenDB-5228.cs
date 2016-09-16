using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5228 : RavenTestBase
    {
        public class ExampleModel
        {
            public string Id;
            public string SearchString;
        }

        public class ExampleIndex : AbstractIndexCreationTask<ExampleModel>
        {
            public ExampleIndex()
            {
                Map = examples => from example in examples
                                  select new
                                  {
                                      Id = example.Id,
                                      SearchString = example.SearchString
                                  };
                Index(x => x.SearchString, FieldIndexing.Analyzed);
            }
        }

        [Fact]
        public void WhereInMultiValueForFieldShouldWork()
        {
            using (DocumentStore store = NewRemoteDocumentStore())
            {
                new ExampleIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ExampleModel
                    {
                        SearchString = "ExampleModels/90ce55c0-d3a8-4982-af33-85a7d525ae01",
                        Id = "a"
                    });
                    session.Store(new ExampleModel
                    {
                        SearchString = "ExampleModels/3be3bdbc-34a1-48d4-aa28-de5bc873d510",
                        Id = "b"
                    });
                    session.Store(new ExampleModel
                    {
                        SearchString = "ExampleModels/2f7392c1-dc2b-4199-9e40-4d4fc6758c92",
                        Id = "c"
                    });
                    session.Store(new ExampleModel
                    {
                        SearchString = "ExampleModels/a04abe17-301a-4a90-b1ce-a6bf7590dd5f",
                        Id = "d"
                    });
                    session.Store(new ExampleModel
                    {
                        SearchString = "ExampleModels/8f6ff28c-6923-4737-b0c1-6e2b7134036",
                        Id = "e"
                    });
                    session.Store(new ExampleModel
                    {
                        SearchString = "ExampleModels/62156-555",
                        Id = "f"
                    });
                    session.Store(new ExampleModel
                    {
                        SearchString = "ExampleModels/7",
                        Id = "g"
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var ids = new List<string> { "ExampleModels/90ce55c0-d3a8-4982-af33-85a7d525ae01", "ExampleModels/3be3bdbc-34a1-48d4-aa28-de5bc873d510", "ExampleModels/2f7392c1-dc2b-4199-9e40-4d4fc6758c92", "ExampleModels/a04abe17-301a-4a90-b1ce-a6bf7590dd5f", "ExampleModels/8f6ff28c-6923-4737-b0c1-6e2b7134036" };
                    var examples1Query = session.Advanced.DocumentQuery<ExampleModel, ExampleIndex>().WhereIn(x => x.SearchString, ids);
                    var examples1 = examples1Query.ToList();

                    ids.Add("ExampleModels/62156-555");
                    var examples2Query = session.Advanced.DocumentQuery<ExampleModel, ExampleIndex>().WhereIn(x => x.SearchString, ids);
                    var examples2 = examples2Query.ToList();

                    ids.Add("ExampleModels/7");
                    var examples3Query = session.Advanced.DocumentQuery<ExampleModel, ExampleIndex>().WhereIn(x => x.SearchString, ids);
                    var examples3 = examples3Query.ToList();

                    Assert.Equal(5, examples1.Count);
                    Assert.Equal(6, examples2.Count);
                    Assert.Equal(7, examples3.Count);
                }
            }
        }
    }
}
