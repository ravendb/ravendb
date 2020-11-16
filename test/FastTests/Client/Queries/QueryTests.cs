using System;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries
{
    public class QueryTests : RavenTestBase
    {
        public QueryTests(ITestOutputHelper output) : base(output)
        {
        }

        private class A
        {
            public B B { get; set; }
        }

        private class B
        {
            public uint Uint { get; set; }
            public long Long { get; set; }
            public ulong Ulong { get; set; }
            public short Short { get; set; }
            public ushort Ushort { get; set; }
            public char Char { get; set; }
            public sbyte Sbyte { get; set; }
            public byte Byte { get; set; }
        }

        [Fact]
        public async Task Query_WhenCompareObjectWithUlongInWhereClause_ShouldWork()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await StoreAsync(session, 2);
                await StoreAsync(session, 1);
                await StoreAsync(session, 1);
                await StoreAsync(session, 0);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                _ = await session.Query<A>().Where(x => x.B == new B { Uint = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Long = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Ulong = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Short = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Ushort = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Char = (char)1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Byte = 1 }).ToArrayAsync();
                _ = await session.Query<A>().Where(x => x.B == new B { Sbyte = 1 }).ToArrayAsync();
            }
        }

        [Fact]
        public async Task Query_WhenUsingDateTimeNowInWhereClause_ShouldSendRequestForEachQuery()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                const int numberOfRequests = 2;
                for (var i = 0; i < numberOfRequests; i++)
                {
                    _ = await session.Query<Order>()
                        .Where(x => x.OrderedAt < DateTime.Now)
                        .Take(5)
                        .ToListAsync();
                }

                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }

        private static async Task StoreAsync(IAsyncDocumentSession session, int value)
        {
            await session.StoreAsync(new A
            {
                B = new B { Uint = (uint)value, Long = value, Ulong = (ulong)value, Short = (short)value, Ushort = (ushort)value, Char = (char)value, Byte = (byte)value, Sbyte = (sbyte)value }
            });
        }
    }
}
