using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Tests.Core;
using Xunit;

namespace FastTests.Client.BulkInsert
{
    public class BulkInserts : RavenTestBase
    {
        [Fact]
        public async Task SimpleBulkInsertShouldWork()
        {
            var fooBars = new FooBar[]
            {
                new FooBar { Name = "John Doe" },
                new FooBar { Name = "Jane Doe" },
                new FooBar { Name = "Mega Joe" }
            };
            using (var store = await GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    await bulkInsert.StoreAsync(fooBars[0]);
                    await bulkInsert.StoreAsync(fooBars[2]);
                    await bulkInsert.StoreAsync(fooBars[1]);
                }								

                //TODO : uncomment this when queries will work
//				using (var session = store.OpenSession())
//				{
//					var fetchedFooBars = session.Query<FooBar>().ToList();
//					Assert.Contains(fooBars[0], fetchedFooBars);
//					Assert.Contains(fooBars[1], fetchedFooBars);
//					Assert.Contains(fooBars[1], fetchedFooBars);
//				}
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
