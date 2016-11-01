// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Indexes;
using Xunit;

namespace FastTests.NewClient.ResultsTransformer
{
    public class AsyncTransformWith : RavenTestBase
    {
        [Fact] // Passes on build 2550
        public void CanRunTransformerOnSession()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new MyTransformer());

                using (var session = store.OpenNewSession())
                {
                    session.Store(new MyModel
                    {
                        Name = "Sherezade",
                        Country = "India",
                        City = "Delhi"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var model = session.Query<MyModel>()
                        .Search(x => x.Name, "Sherezade")
                        .TransformWith<MyTransformer, MyModelProjection>()
                        .FirstOrDefault();

                    Assert.Equal("Sherezade", model.Name);
                    Assert.Equal("India,Delhi", model.CountryAndCity);
                }
            }
        }

        [Fact] // Fails on build 2550        
        public async Task CanRunTransformerOnAsyncSession()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new MyTransformer());

                using (var session = store.OpenNewSession())
                {
                    session.Store(new MyModel
                    {
                        Name = "Sherezade",
                        Country = "India",
                        City = "Delhi"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenNewAsyncSession())
                {
                    var model = await session.Query<MyModel>()
                        .Search(x => x.Name, "Sherezade")
                        .TransformWith<MyTransformer, MyModelProjection>()
                        .FirstOrDefaultAsync();

                    Assert.Equal("Sherezade", model.Name);
                    Assert.Equal("India,Delhi", model.CountryAndCity);
                }
            }
        }

        public class MyModel
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Country { get; set; }
            public string City { get; set; }
        }

        public class MyModelProjection
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string CountryAndCity { get; set; }
        }

        public class MyTransformer : AbstractTransformerCreationTask<MyModel>
        {
            public MyTransformer()
            {
                TransformResults = docus => from d in docus
                                            select new
                                            {
                                                d.Id,
                                                d.Name,
                                                CountryAndCity = String.Join(",", d.Country, d.City)
                                            };
            }
        }
    }
}
