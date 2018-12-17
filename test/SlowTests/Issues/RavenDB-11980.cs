using System.Collections.Generic;
using FastTests;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11980 : RavenTestBase
    {
        private class Matter
        {
            public string Title { get; set; }

            public string Author { get; set; }

            public Dictionary<string, string> CustomFields { get; set; }
        }

        private class Filter_key_Index : AbstractIndexCreationTask<Matter>
        {
            public Filter_key_Index()
            {
                Map = matters =>
                    from e in matters
                    select new
                    {
                        system_Key_Index =
                            e.CustomFields.Where(x => x.Key == "filter_key")
                                .Select(x => new KeyValuePair<string, string>(x.Key + "some_Text", x.Value.ToString()))
                    };
            }
        }

        private class Index_With_DynamicBlittableJson_Extension_Methods : AbstractIndexCreationTask<Matter>
        {
            public Index_With_DynamicBlittableJson_Extension_Methods()
            {
                Map = matters =>
                    from e in matters
                    select new
                    {
                        GroupBy = e.CustomFields.GroupBy(kvp => kvp.Value),
                        OrderByDescending = e.CustomFields.OrderByDescending(kvp => kvp.Value),
                        DefaultIfEmpty = e.CustomFields.DefaultIfEmpty(),
                        Reverse = e.CustomFields.Reverse(),
                        Take = e.CustomFields.Take(1),
                        Skip = e.CustomFields.Skip(1),
                        OfType = e.CustomFields.OfType<KeyValuePair<object, object>>()
                    };
            }
        }

        [Fact]
        public void Can_Use_DynamicBlittableJson_Where()
        {
            using (var store = GetDocumentStore())
            {
                new Filter_key_Index().Execute(store);

                List<Matter> books = new List<Matter>();
                books.Add(new Matter
                {
                    Title = "abc",
                    Author = "john",
                    CustomFields = new Dictionary<string, string>
                    {
                        { "filter_key", "value1" },
                        { "some_other_key", "value2" }
                    }
                });
                books.Add(new Matter
                {
                    Title = "xyz",
                    Author = "doe",
                    CustomFields = new Dictionary<string, string>
                    {
                        { "filter_key", "value3" },
                        { "some_other_key", "value4" }
                    }
                });

                using (var s = store.OpenSession())
                {
                    s.Store(books.First());
                    s.Store(books.Last());

                    s.Advanced.WaitForIndexesAfterSaveChanges();
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var booksByJohn = s.Query<Matter, Filter_key_Index>().ToList();
                    Assert.Equal(2, booksByJohn.Count);
                }

            }
        }

        [Fact]
        public void Can_Use_DynamicBlittableJson_ExtensionMethods()
        {
            using (var store = GetDocumentStore())
            {
                new Index_With_DynamicBlittableJson_Extension_Methods().Execute(store);

                List<Matter> books = new List<Matter>();
                books.Add(new Matter
                {
                    Title = "abc",
                    Author = "john",
                    CustomFields = new Dictionary<string, string>
                    {
                        { "filter_key", "value1" },
                        { "some_other_key", "value2" }
                    }
                });
                books.Add(new Matter
                {
                    Title = "xyz",
                    Author = "doe",
                    CustomFields = new Dictionary<string, string>
                    {
                        { "filter_key", "value3" },
                        { "some_other_key", "value4" }
                    }
                });

                using (var s = store.OpenSession())
                {
                    s.Store(books.First());
                    s.Store(books.Last());

                    s.Advanced.WaitForIndexesAfterSaveChanges();
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var booksByJohn = s.Query<Matter, Index_With_DynamicBlittableJson_Extension_Methods>().ToList();

                    Assert.Equal(2, booksByJohn.Count);
                }

            }
        }
    }
}
