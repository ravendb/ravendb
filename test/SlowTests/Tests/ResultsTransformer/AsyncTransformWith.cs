// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.ResultsTransformer
{
    public class AsyncTransformWith : RavenNewTestBase
    {
        [Fact] // Passes on build 2550
        public void CanRunTransformerOnSession()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new MyTransformer());

                using (var session = store.OpenSession())
                {
                    session.Store(new MyModel
                    {
                        Name = "Sherezade",
                        Country = "India",
                        City = "Delhi"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
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

                using (var session = store.OpenSession())
                {
                    session.Store(new MyModel
                    {
                        Name = "Sherezade",
                        Country = "India",
                        City = "Delhi"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
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

        private class MyModel
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Country { get; set; }
            public string City { get; set; }
        }

        private class MyModelProjection
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string CountryAndCity { get; set; }
        }

        private class MyTransformer : AbstractTransformerCreationTask<MyModel>
        {
            public MyTransformer()
            {
                TransformResults = docus => from d in docus
                                            select new
                                            {
                                                d.Id,
                                                d.Name,
                                                CountryAndCity = string.Join(",", d.Country, d.City)
                                            };
            }
        }
    }
}
