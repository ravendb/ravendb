// -----------------------------------------------------------------------
//  <copyright file="RavenDB987.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB987 : RavenTestBase
    {
        private sealed class Categories_InUse_ByCity : AbstractIndexCreationTask<Restaurant, Categories_InUse_ByCity.Result>
        {
            public Categories_InUse_ByCity()
            {
                Map = restaurants => from r in restaurants
                                     from c in r.Categories.Union(new[] { r.MainCategory })
                                     let categ = LoadDocument<Category>(c)
                                     select new
                                     {
                                         categ.Id,
                                         categ.Name,
                                         categ.Icon,
                                         categ.Color,
                                         categ.Index,
                                         r.CityId,
                                         r.CityName,
                                     };

                Reduce = results => from r in results
                                    group r by r.Id
                                    into g
                                    let r = g.First()
                                    select new
                                    {
                                        r.Id,
                                        r.Name,
                                        r.Icon,
                                        r.Color,
                                        r.Index,
                                        r.CityId,
                                        r.CityName,
                                    };

            }

            public sealed class Result
            {
                public string Id { get; set; }

                public string Name { get; set; }

                public string Icon { get; set; }

                public string Color { get; set; }

                public int Index { get; set; }


                public string CityId { get; set; }

                public string CityName { get; set; }
            }
        }

        private class Category
        {
            public string Name { get; set; }
            public string Icon { get; set; }
            public string Color { get; set; }
            public int Index { get; set; }
            public string Id { get; set; }
        }

        private class Restaurant
        {
            public string Id { get; set; }
            public string MainCategory { get; set; }
            public List<string> Categories { get; set; }
            public string CityId { get; set; }
            public string CityName { get; set; }
        }

        [Fact]
        public void ShouldGetAppropriateResults()
        {
            using (var store = GetDocumentStore())
            {
                new Categories_InUse_ByCity().Execute(store);

                using (var sesssion = store.OpenSession())
                {
                    for (int i = 0; i < 6; i++)
                    {
                        sesssion.Store(new Category
                        {
                            Color = "red",
                            Icon = "foo.jpg",
                            Index = i,
                            Name = "test " + i
                        });
                    }

                    for (int i = 0; i < 3; i++)
                    {
                        sesssion.Store(new Restaurant
                        {
                            Categories = new List<string> { "categories/1-A", "categories/2-A", "categories/3-A", "categories/4-A", "categories/5-A" },
                            CityId = "cities/2",
                            CityName = "New York",
                            MainCategory = "categories/6-A"
                        });
                    }

                    sesssion.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Categories_InUse_ByCity.Result, Categories_InUse_ByCity>()
                           .Customize(x => x.WaitForNonStaleResults())
                           .ToList();

                    Assert.Equal(6, results.Count);

                    RavenTestHelper.AssertNoIndexErrors(store);
                }
            }
        }
    }
}
