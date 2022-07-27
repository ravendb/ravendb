using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13060 : RavenTestBase
    {
        public RavenDB_13060(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void Can_project_using_let_and_have_nested_query_with_let_that_refers_to_the_outer_let(Options options)
        {
            using (var store = GetDocumentStore(options))
            using (var session = store.OpenSession())
            {
                const string categoryListId = "reference/categoryList";
                const string departmentListId = "reference/departmentList";

                var categories = new CategoryList
                {
                    Categories = new Dictionary<string, Category>
                    {
                        { "1", new Category { Name = "Tables" } },
                        { "2", new Category { Name = "Electrical" } }
                    }
                };
                session.Store(categories, categoryListId);

                var departments = new DepartmentList
                {
                    Departments = new Dictionary<string, Department>
                    {
                        { "1", new Department { Name = "Operations" } },
                        { "2", new Department { Name = "Engineering" } }
                    }
                };
                session.Store(departments, departmentListId);

                var order = new Order
                {
                    Items = new List<OrderItem>
                    {
                        new OrderItem { Name = "Table", CategoryId = "1", DepartmentId = "1" },
                        new OrderItem { Name = "20 Amp Service", CategoryId = "2", DepartmentId = "2" }
                    }
                };
                session.Store(order);
                session.SaveChanges();

                var query = from o in session.Query<Order>()
                            let categoryList = RavenQuery.Load<CategoryList>(categoryListId)
                            let departmentList = RavenQuery.Load<DepartmentList>(departmentListId)
                            select new
                            {
                                o.Id,
                                Items = from i in order.Items
                                        let category = categoryList.Categories[i.CategoryId]
                                        let department = departmentList.Departments[i.DepartmentId]
                                        select new
                                        {
                                            i.Name,
                                            CategoryName = category.Name,
                                            DepartmentName = department.Name
                                        }
                            };

                var result = query.First();

                Assert.All(result.Items, item =>
                {
                    Assert.NotNull(item.CategoryName);
                    Assert.NotNull(item.DepartmentName);
                });
            }

        }


        private class CategoryList
        {
            public IDictionary<string, Category> Categories { get; set; }
        }

        private class Category
        {
            public string Name { get; set; }
        }

        private class DepartmentList
        {
            public IDictionary<string, Department> Departments { get; set; }
        }

        private class Department
        {
            public string Name { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public IEnumerable<OrderItem> Items { get; set; }
        }

        private class OrderItem
        {
            public string Name { get; set; }
            public string CategoryId { get; set; }
            public string DepartmentId { get; set; }
        }
    }
}
