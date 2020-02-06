using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;
// ReSharper disable IdentifierTypo

namespace FastTests.Issues
{
    public class RavenDB_14576 : RavenTestBase
    {
        public RavenDB_14576(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TypeConverterShouldFlattenArrayOfArrays()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                using var blittable1 = ctx.ReadObject(new DynamicJsonValue {["Age"] = 1}, "1");
                using var blittable2 = ctx.ReadObject(new DynamicJsonValue {["Age"] = 2}, "2");
                using var blittable3 = ctx.ReadObject(new DynamicJsonValue {["Age"] = 3}, "3");

                var arr1 = new DynamicArray(new[] { blittable1, blittable2 });
                var arr2 = new DynamicArray(new[] { blittable3 });
                var array = new DynamicArray(new[] { arr1, arr2 });
                var newArray = arr1.Concat(arr2);

                var flattered = TypeConverter.Flatten(array);
                var count = 0;

                foreach (var bjro in flattered)
                {
                    Assert.Contains(bjro, newArray);
                    count++;
                }

                Assert.Equal(3, count);
            }
        }

        [Fact]
        public void JavascriptIndexShouldPassContextToTypeConverter()
        {
            using (var store = GetDocumentStore())
            {
                using var session = store.OpenSession();
                for (int i = 0; i < 500; i++)
                {
                    session.Store(new User
                    {
                        UserName = $"EGOR_{i}",
                        Etag = Guid.NewGuid().ToString(),
                        Type = "Support",
                        NestedItems = new NestedItem
                        {
                            NestedItemName = "EGOR",
                            NestedItemId = 322
                        }
                    }, Guid.NewGuid().ToString());
                }
                session.SaveChanges();

                new JavascriptIndex().Execute(store);
                WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5), allowErrors: true);

                var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation());
                Assert.Empty(indexErrors.First().Errors);
            }
        }

        [Fact]
        public void JavascriptIndexShouldThrowOnExceptionInReduceStage()
        {
            using (var store = GetDocumentStore())
            {
                using var session = store.OpenSession();
                for (int i = 0; i < 500; i++)
                {
                    session.Store(new User
                    {
                        UserName = $"EGOR_{i}",
                        Etag = Guid.NewGuid().ToString(),
                        Type = "Support",
                        NestedItems = new NestedItem
                        {
                            NestedItemName = "EGOR",
                            NestedItemId = 322
                        }
                    }, Guid.NewGuid().ToString());
                }
                session.SaveChanges();

                new JavascriptIndex2().Execute(store);
                WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5), allowErrors: true);

                var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation());
                var exceptions = indexErrors.First().Errors;
                Assert.NotEmpty(exceptions);
                var firstException = exceptions.First();
                Assert.StartsWith("Failed to execute reduce function", firstException.Error);
            }
        }

        private class JavascriptIndex : AbstractIndexCreationTask
        {
            public override string IndexName => "JavascriptIndex";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"map(""Users"", (user) => {
    return {
        ParentId: user.NestedItems.NestedItemId,
        ParentName: user.NestedItems.NestedItemName,
        ChildDocument: {
            Name: user.Name,
            Id: id(user),
            Etag: user.Etag,
            Type: user.Type
        }
    };
})"
                    },
                    Reduce = @"groupBy(x => ({
    ParentId: x.ParentId,
    ParentName: x.ParentName
})).aggregate(g => {
    var arr = g.values.reduce((acc, val) => {
        acc.push(val.ChildDocument);
        return acc; }, []);
        return {
            ParentId: g.key.ParentId,
            ParentName: g.key.ParentName,
            ChildFolderOrDocument: arr
        };
    })"
                };
            }
        }

        private class JavascriptIndex2 : AbstractIndexCreationTask
        {
            public override string IndexName => "JavascriptIndex2";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"map(""Users"", (user) => {
    return {
        ParentId: user.NestedItems.NestedItemId,
        ParentName: user.NestedItems.NestedItemName,
        Count: 1
    };
})"
                    },
                    Reduce = @"groupBy(x => ({
    ParentId: x.ParentId,
    ParentName: x.ParentName
})).aggregate(g => {
    var count = g.values.reduce((count, val) => val.Count + count, 0);
        return {
            ParentId: g.key.ParentId,
            ParentName: g.key.ParentName,
            Count: count.substring(1, 4)
        };
    })"
                };
            }
        }

        private class User
        {
            public string UserName { get; set; }
            public NestedItem NestedItems { get; set; }
            public string Etag { get; set; }
            public string Type { get; set; }
        }

        private class NestedItem
        {
            public string NestedItemName { get; set; }
            public int NestedItemId { get; set; }
        }
    }
}
