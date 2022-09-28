using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19286 : RavenTestBase
{
    public RavenDB_19286(ITestOutputHelper output) : base(output)
    {
    }

    class User
    {
        public string Name;
    }

    [Fact]
    public async Task CanDoStringRangeQuery()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Zoof" });
            await session.StoreAsync(new User { Name = "Aoof" });
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var equalByCompareTo = await session.Query<User>()
                .Where(x => x.Name.CompareTo( "Zoof") == 0)
                .ToListAsync();
            Assert.Equal(1, equalByCompareTo.Count);
            
            
            var equalByStringCompare = await session.Query<User>()
                .Where(x => string.Compare(x.Name, "Zoof") == 0)
                .ToListAsync();
            Assert.Equal(1, equalByStringCompare.Count);
            
            
            var lessThanByCompareTo = await session.Query<User>()
                .Where(x => x.Name.CompareTo( "Zoof") < 0)
                .ToListAsync();
            Assert.Equal(1, lessThanByCompareTo.Count);
            
            var lessThanByStringCompare = await session.Query<User>()
                .Where(x => string.Compare(x.Name, "Zoof") < 0)
                .ToListAsync();
            Assert.Equal(1, lessThanByStringCompare.Count);
            
            
            var lessOrEqualsThanByCompareTo = await session.Query<User>()
                .Where(x => x.Name.CompareTo( "Zoof") <= 0)
                .ToListAsync();
            Assert.Equal(2, lessOrEqualsThanByCompareTo.Count);
            
            var lessOrEqualsThanByStringCompare = await session.Query<User>()
                .Where(x => string.Compare(x.Name, "Zoof") <= 0)
                .ToListAsync();
            Assert.Equal(2, lessOrEqualsThanByStringCompare.Count);
            
            
            var greaterThanByCompareTo = await session.Query<User>()
                .Where(x => x.Name.CompareTo( "Zoof") > 0)
                .ToListAsync();
            Assert.Equal(0, greaterThanByCompareTo.Count);
            
            var greaterThanByStringCompare = await session.Query<User>()
                .Where(x => string.Compare(x.Name, "Zoof") > 0)
                .ToListAsync();
            Assert.Equal(0, greaterThanByStringCompare.Count);
            
            
            var greaterOrEqualsThanByCompareTo = await session.Query<User>()
                .Where(x => x.Name.CompareTo( "Zoof") >= 0)
                .ToListAsync();
            Assert.Equal(1, greaterOrEqualsThanByCompareTo.Count);
            
            var greaterOrEqualsThanByStringCompare = await session.Query<User>()
                .Where(x => string.Compare(x.Name, "Zoof") >= 0)
                .ToListAsync();
            Assert.Equal(1, greaterOrEqualsThanByStringCompare.Count);
            
            var notEqualsThanByCompareTo = await session.Query<User>()
                .Where(x => x.Name.CompareTo( "Zoof") != 0)
                .ToListAsync();
            Assert.Equal(1, notEqualsThanByCompareTo.Count);
            
            var notEqualsThanByStringCompare = await session.Query<User>()
                .Where(x => string.Compare(x.Name, "Zoof") != 0)
                .ToListAsync();
            Assert.Equal(1, notEqualsThanByStringCompare.Count);
        }
    }
}
