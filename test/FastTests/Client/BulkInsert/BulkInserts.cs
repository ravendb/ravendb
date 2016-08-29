using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Raven.Tests.Core;
using Xunit;

namespace FastTests.Client.BulkInsert
{
    public class BulkInserts : RavenTestBase
    {
        [Fact]
        public async Task SimpleBulkInsertShouldWork()
        {
            var fooBars = new[]
            {
                new FooBar { Name = "John Doe" },
                new FooBar { Name = "Jane Doe" },
                new FooBar { Name = "Mega John" },
                new FooBar { Name = "Mega Jane" }
            };
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    await bulkInsert.StoreAsync(fooBars[0]);
                    await bulkInsert.StoreAsync(fooBars[1]);
                    await bulkInsert.StoreAsync(fooBars[2]);
                    await bulkInsert.StoreAsync(fooBars[3]);
                }

                var doc1 = store.DatabaseCommands.Get("FooBars/1");
                var doc2 = store.DatabaseCommands.Get("FooBars/2");
                var doc3 = store.DatabaseCommands.Get("FooBars/3");
                var doc4 = store.DatabaseCommands.Get("FooBars/4");

                Assert.NotNull(doc1);
                Assert.NotNull(doc2);
                Assert.NotNull(doc3);
                Assert.NotNull(doc4);

                Assert.Equal("John Doe", doc1.DataAsJson.Value<string>("Name"));
                Assert.Equal("Jane Doe", doc2.DataAsJson.Value<string>("Name"));
                Assert.Equal("Mega John", doc3.DataAsJson.Value<string>("Name"));
                Assert.Equal("Mega Jane", doc4.DataAsJson.Value<string>("Name"));
            }
        }

        public class FooBarIndex : AbstractIndexCreationTask<FooBar>
        {
            public FooBarIndex()
            {
                Map = foos => foos.Select(x => new { x.Name });
            }
        }

        public class FooBar : IEquatable<FooBar>
        {
            public string Name { get; set; }

            public bool Equals(FooBar other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(Name, other.Name);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((FooBar) obj);
            }

            public override int GetHashCode()
            {
                return Name?.GetHashCode() ?? 0;
            }

            public static bool operator ==(FooBar left, FooBar right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(FooBar left, FooBar right)
            {
                return !Equals(left, right);
            }
        }
    }
}
