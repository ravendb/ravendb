using Tests.Infrastructure;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11767 : RavenTestBase
    {
        public RavenDB_11767(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ShouldSimplifyTransparentIdentifierParameters(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var categories = Enumerable.Range(0, 10)
                        .Select(x =>
                        {
                            var category = new Category { Name = $"Category {x}" };
                            session.Store(category);
                            return category;
                        })
                        .ToList();

                    var categoryList = new CategoryList { Categories = categories };
                    session.Store(categoryList);

                    var orderItems = categories
                        .Select(x => new OrderItem { CategoryId = x.Id })
                        .ToList();

                    var order = new Order { CategoryListId = categoryList.Id, OrderItems = orderItems };
                    session.Store(order);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var queryable = from o in session.Query<Order>()
                                    let categoryList = RavenQuery.Load<CategoryList>(o.CategoryListId)
                                    select new OrderProjection
                                    {
                                        Id = o.Id,
                                        Items = from i in o.OrderItems
                                                let cat = categoryList.Categories
                                                let id = i.CategoryId
                                                let first = cat.FirstOrDefault(x => x.Id == id)
                                                let name = first.Name
                                                select new OrderItemProjection
                                                {
                                                    CategoryName = name
                                                }
                                    };

                    Assert.Equal("from 'Orders' as o load o?.CategoryListId as categoryList " +
                                 "select { Id : id(o), Items : ((((((((((o?.OrderItems" +
                                    "??[]).map(function(i){return {i:i,cat:categoryList?.Categories};}))" +
                                    "??[]).map(function(__rvn0){return {__rvn0:__rvn0,id:(__rvn0?.i)?.CategoryId};}))" +
                                    "??[]).map(function(__rvn1){return {__rvn1:__rvn1,first:(((__rvn1?.__rvn0)?.cat)?.find(function(x){return id(x)===(__rvn1?.id);}))};}))" +
                                    "??[]).map(function(__rvn2){return {__rvn2:__rvn2,name:(__rvn2?.first)?.Name};}))" +
                                    "??[]).map(function(__rvn3){return {CategoryName:(__rvn3?.name)};})) }"
                                , queryable.ToString());

                    var result = queryable.ToList();
                    Assert.NotNull(result);

                    var items = result[0].Items.ToList();
                    Assert.Equal(10, items.Count);

                    for (var i = 0; i < 10; i++)
                    {
                        Assert.Equal($"Category {i}", items[i].CategoryName);
                    }
                }
            }
        }

        private class Order
        {
            public string Id { get; set; }
            public string CategoryListId { get; set; }
            public IEnumerable<OrderItem> OrderItems { get; set; }
        }

        private class OrderItem
        {
            public string Id { get; set; }
            public string CategoryId { get; set; }
        }

        private class CategoryList
        {
            public string Id { get; set; }
            public IEnumerable<Category> Categories { get; set; }
        }

        private class Category
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class OrderProjection
        {
            public string Id { get; set; }
            public IEnumerable<OrderItemProjection> Items { get; set; }
        }

        private class OrderItemProjection
        {
            public string CategoryName { get; set; }
        }

    }
}
