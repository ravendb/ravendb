using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15166 : RavenTestBase
    {
        public RavenDB_15166(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Translate_String_Compare()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Adam" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Ariel" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Dan" }, "users/3");
                    await asyncSession.StoreAsync(new User { Name = "Rachel" }, "users/4");
                    await asyncSession.SaveChangesAsync();

                    var queryResultLT = await asyncSession.Query<User>()
                        .Where(x => x.Name.CompareTo("Dan") < 0)
                        .ToListAsync();

                    var queryResultGT = await asyncSession.Query<User>()
                        .Where(x => x.Name.CompareTo("Dan") > 0)
                        .ToListAsync();

                    var queryResultLE = await asyncSession.Query<User>()
                        .Where(x => x.Name.CompareTo("Dan") <= 0)
                        .ToListAsync();

                    var queryResultGE = await asyncSession.Query<User>()
                        .Where(x => x.Name.CompareTo("Dan") >= 0)
                        .ToListAsync();

                    Assert.Equal(2, queryResultLT.Count);
                    Assert.Contains("Adam", queryResultLT.Select(r => r.Name));
                    Assert.Contains("Ariel", queryResultLT.Select(r => r.Name));

                    Assert.Equal(1, queryResultGT.Count);
                    Assert.Contains("Rachel", queryResultGT.Select(r => r.Name));

                    Assert.Equal(queryResultLE.Count, 3);
                    Assert.Contains("Adam", queryResultLE.Select(r => r.Name));
                    Assert.Contains("Ariel", queryResultLE.Select(r => r.Name));
                    Assert.Contains("Dan", queryResultLE.Select(r => r.Name));

                    Assert.Equal(queryResultGE.Count, 2);
                    Assert.Contains("Dan", queryResultGE.Select(r => r.Name));
                    Assert.Contains("Rachel", queryResultGE.Select(r => r.Name));
                }
            }
        }

        [Fact]
        public async Task Can_Translate_String_Compare_Using_Static_Compare()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Adam" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Ariel" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Dan" }, "users/3");
                    await asyncSession.StoreAsync(new User { Name = "Rachel" }, "users/4");
                    await asyncSession.SaveChangesAsync();

                    var queryResultLT = await asyncSession.Query<User>()
                        .Where(x => string.Compare(x.Name, "Dan") < 0)
                        .ToListAsync();

                    var queryResultGT = await asyncSession.Query<User>()
                        .Where(x => string.Compare(x.Name, "Dan") > 0)
                        .ToListAsync();

                    var queryResultLE = await asyncSession.Query<User>()
                        .Where(x => string.Compare(x.Name, "Dan") <= 0)
                        .ToListAsync();

                    var queryResultGE = await asyncSession.Query<User>()
                        .Where(x => string.Compare(x.Name, "Dan") >= 0)
                        .ToListAsync();

                    Assert.Equal(2, queryResultLT.Count);
                    Assert.Contains("Adam", queryResultLT.Select(r => r.Name));
                    Assert.Contains("Ariel", queryResultLT.Select(r => r.Name));

                    Assert.Equal(1, queryResultGT.Count);
                    Assert.Contains("Rachel", queryResultGT.Select(r => r.Name));

                    Assert.Equal(queryResultLE.Count, 3);
                    Assert.Contains("Adam", queryResultLE.Select(r => r.Name));
                    Assert.Contains("Ariel", queryResultLE.Select(r => r.Name));
                    Assert.Contains("Dan", queryResultLE.Select(r => r.Name));

                    Assert.Equal(queryResultGE.Count, 2);
                    Assert.Contains("Dan", queryResultGE.Select(r => r.Name));
                    Assert.Contains("Rachel", queryResultGE.Select(r => r.Name));
                }
            }
        }

        [Fact]
        public async Task Can_Translate_String_Compare_Using_Static_Compare_When_String_Constant_Comes_First()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Adam" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Ariel" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Dan" }, "users/3");
                    await asyncSession.StoreAsync(new User { Name = "Rachel" }, "users/4");
                    await asyncSession.SaveChangesAsync();

                    var queryResultLT = await asyncSession.Query<User>()
                        .Where(x => string.Compare("Dan", x.Name) < 0)
                        .ToListAsync();

                    var queryResultGT = await asyncSession.Query<User>()
                        .Where(x => string.Compare("Dan", x.Name) > 0)
                        .ToListAsync();

                    var queryResultLE = await asyncSession.Query<User>()
                        .Where(x => string.Compare("Dan", x.Name) <= 0)
                        .ToListAsync();

                    var queryResultGE = await asyncSession.Query<User>()
                        .Where(x => string.Compare("Dan", x.Name) >= 0)
                        .ToListAsync();

                    Assert.Equal(2, queryResultLT.Count);
                    Assert.Contains("Adam", queryResultLT.Select(r => r.Name));
                    Assert.Contains("Ariel", queryResultLT.Select(r => r.Name));

                    Assert.Equal(1, queryResultGT.Count);
                    Assert.Contains("Rachel", queryResultGT.Select(r => r.Name));

                    Assert.Equal(queryResultLE.Count, 3);
                    Assert.Contains("Adam", queryResultLE.Select(r => r.Name));
                    Assert.Contains("Ariel", queryResultLE.Select(r => r.Name));
                    Assert.Contains("Dan", queryResultLE.Select(r => r.Name));

                    Assert.Equal(queryResultGE.Count, 2);
                    Assert.Contains("Dan", queryResultGE.Select(r => r.Name));
                    Assert.Contains("Rachel", queryResultGE.Select(r => r.Name));
                }
            }
        }

        [Fact]
        public async Task Can_Translate_String_Compare_Using_Static_Compare_When_String_Constant_And_Compare_Constant_Comes_First()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Adam" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Ariel" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Dan" }, "users/3");
                    await asyncSession.StoreAsync(new User { Name = "Rachel" }, "users/4");
                    await asyncSession.SaveChangesAsync();

                    var queryResultLT = await asyncSession.Query<User>()
                        .Where(x => 0 > string.Compare("Dan", x.Name))
                        .ToListAsync();

                    var queryResultGT = await asyncSession.Query<User>()
                        .Where(x => 0 < string.Compare("Dan", x.Name))
                        .ToListAsync();

                    var queryResultLE = await asyncSession.Query<User>()
                        .Where(x => 0 >= string.Compare("Dan", x.Name))
                        .ToListAsync();

                    var queryResultGE = await asyncSession.Query<User>()
                        .Where(x => 0 <= string.Compare("Dan", x.Name))
                        .ToListAsync();

                    Assert.Equal(2, queryResultLT.Count);
                    Assert.Contains("Adam", queryResultLT.Select(r => r.Name));
                    Assert.Contains("Ariel", queryResultLT.Select(r => r.Name));

                    Assert.Equal(1, queryResultGT.Count);
                    Assert.Contains("Rachel", queryResultGT.Select(r => r.Name));

                    Assert.Equal(queryResultLE.Count, 3);
                    Assert.Contains("Adam", queryResultLE.Select(r => r.Name));
                    Assert.Contains("Ariel", queryResultLE.Select(r => r.Name));
                    Assert.Contains("Dan", queryResultLE.Select(r => r.Name));

                    Assert.Equal(queryResultGE.Count, 2);
                    Assert.Contains("Dan", queryResultGE.Select(r => r.Name));
                    Assert.Contains("Rachel", queryResultGE.Select(r => r.Name));
                }
            }
        }

        [Fact]
        public async Task Can_Translate_String_Compare_When_Constant_Comes_First()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Adam" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Ariel" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Dan" }, "users/3");
                    await asyncSession.StoreAsync(new User { Name = "Rachel" }, "users/4");
                    await asyncSession.SaveChangesAsync();

                    var queryResultLT = await asyncSession.Query<User>()
                        .Where(x => 0 > x.Name.CompareTo("Dan"))
                        .ToListAsync();

                    Assert.Equal(queryResultLT.Count, 2);
                    Assert.Contains("Adam", queryResultLT.Select(r => r.Name));
                    Assert.Contains("Ariel", queryResultLT.Select(r => r.Name));
                }
            }
        }

        [Fact]
        public async Task Can_Translate_String_Compare_When_Comparing_Null()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Adam" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Ariel" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Dan" }, "users/3");
                    await asyncSession.StoreAsync(new User { Name = "Rachel" }, "users/4");
                    await asyncSession.SaveChangesAsync();

                    var queryResultLT = await asyncSession.Query<User>()
                        .Where(x => x.Name.CompareTo(null) > 0)
                        .ToListAsync();

                    Assert.Equal(4, queryResultLT.Count);
                }
            }
        }


        [Fact]
        public async Task Can_Translate_String_Compare_Throws_When_Comparison_Is_Not_Zero()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Adam" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Ariel" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Dan" }, "users/3");
                    await asyncSession.StoreAsync(new User { Name = "Rachel" }, "users/4");
                    await asyncSession.SaveChangesAsync();

                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => x.Name.CompareTo("Dan") > 1)
                            .ToListAsync();
                    });

                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => -6 < x.Name.CompareTo("Dan"))
                            .ToListAsync();
                    });

                    int zero = 0;

                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => x.Name.CompareTo("Dan") < zero)
                            .ToListAsync();
                    });

                    Func<int> returnZero = () => 0;

                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => x.Name.CompareTo("Dan") < returnZero())
                            .ToListAsync();
                    });
                }
            }
        }


        [Fact]
        public async Task Can_Translate_String_Compare_Throws_When_No_MemberAccess()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Adam" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Ariel" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Dan" }, "users/3");
                    await asyncSession.StoreAsync(new User { Name = "Rachel" }, "users/4");
                    await asyncSession.SaveChangesAsync();

                    await Assert.ThrowsAsync<NotSupportedException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => "Dave".CompareTo("Dan") > 0)
                            .ToListAsync();
                    });

                    string dan = "Dan";

                    await Assert.ThrowsAsync<NotSupportedException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => "Dave".CompareTo(dan) > 0)
                            .ToListAsync();
                    });

                    string dave = "Dave";

                    await Assert.ThrowsAsync<NotSupportedException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => dave.CompareTo("Dan") > 0)
                            .ToListAsync();
                    });

                    await Assert.ThrowsAsync<NotSupportedException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => string.Compare("Dave", "Dan") > 0)
                            .ToListAsync();
                    });
                }
            }
        }

        [Fact]
        public async Task Can_Translate_String_Compare_Throws_When_Comparing_To_Non_Constant()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Adam" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Ariel" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Dan" }, "users/3");
                    await asyncSession.StoreAsync(new User { Name = "Rachel" }, "users/4");
                    await asyncSession.SaveChangesAsync();
                    
                    string dan = "Dan";

                    await Assert.ThrowsAsync<NotSupportedException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => x.Name.CompareTo(dan) > 0)
                            .ToListAsync();
                    });

                    Func<string> dave = () => "Dave";

                    await Assert.ThrowsAsync<NotSupportedException>(async () =>
                    {
                        await asyncSession.Query<User>()
                            .Where(x => x.Name.CompareTo(dave()) > 0)
                            .ToListAsync();
                    });
                }
            }
        }
    }
}
