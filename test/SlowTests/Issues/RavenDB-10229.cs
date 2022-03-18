using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10229 : RavenTestBase
    {
        public RavenDB_10229(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryDateTime()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DateIndex());

                var now = DateTime.Now;
                using (var session = store.OpenSession())
                {
                    session.Store(new Date
                    {
                        DateTime = now
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<DateIndex.Result, DateIndex>()
                        .Where(x => x.DateTime == now)
                        .As<Date>()
                        .ToList();

                    Assert.Equal(1, list.Count);
                    Assert.Equal(now, list[0].DateTime);
                }
            }
        }

        [Fact]
        public void CanIndexDateTimeArrayProperly1()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new LastAccessIndex());

                var dateTime = DateTime.Now.AddDays(4);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel",
                        LoginsByDay = new Dictionary<DateTime, long>
                        {
                            { DateTime.Now, 3},
                            { DateTime.Now.AddDays(-1), 3},
                            { DateTime.Now.AddDays(-2), 3},
                            { DateTime.Now.AddDays(-3), 3}
                        }
                    });

                    session.Store(new User
                    {
                        Name = "Grisha",
                        LoginsByDay = new Dictionary<DateTime, long>
                        {
                            { DateTime.Now, 3},
                            { DateTime.Now.AddDays(1), 3},
                            { DateTime.Now.AddDays(2), 3},
                            { DateTime.Now.AddDays(3), 3},
                            { dateTime, 3},
                        }
                    });

                    session.SaveChanges();
                }
                WaitForUserToContinueTheTest(store);
                Indexes.WaitForIndexing(store);


                using (var session = store.OpenSession())
                {
                    var list = session.Query<LastAccessIndex.Result, LastAccessIndex>()
                        .Where(x => x.LastAccess.Any(d => d == dateTime))
                        .As<User>()
                        .ToList();

                    WaitForUserToContinueTheTest(store);

                    Assert.Equal(1, list.Count);
                    Assert.Equal("Grisha", list[0].Name);
                    Assert.Equal(dateTime, list[0].LoginsByDay.Last().Key);
                }
            }
        }

        [Fact]
        public void CanIndexDateTimeArrayProperly2()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new LastAccessPerName());

                var dateTime = DateTime.Now.AddDays(4);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel",
                        LoginsByDay = new Dictionary<DateTime, long>
                        {
                            { DateTime.Now, 3},
                            { DateTime.Now.AddDays(-1), 3},
                            { DateTime.Now.AddDays(-2), 3},
                            { DateTime.Now.AddDays(-3), 3}
                        }
                    });

                    session.Store(new User
                    {
                        Name = "Grisha",
                        LoginsByDay = new Dictionary<DateTime, long>
                        {
                            { DateTime.Now, 3},
                            { DateTime.Now.AddDays(1), 3},
                            { DateTime.Now.AddDays(2), 3},
                            { DateTime.Now.AddDays(3), 3},
                            { dateTime, 3},
                        }
                    });

                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<LastAccessPerName.Result, LastAccessPerName>()
                        .Where(x => x.LastAccess.Any(d => d == dateTime))
                        .ToList();

                    Assert.Equal(1, list.Count);
                    Assert.Equal("Grisha", list[0].Name);
                    Assert.Equal(dateTime, list[0].LastAccess.First());
                }
            }
        }

        [Fact]
        public void CanIndexGuidArrayProperly1()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DummyIndex());

                var newGuid = Guid.NewGuid();
                using (var session = store.OpenSession())
                {
                    session.Store(new Dummy
                    {
                        Guid = newGuid
                    });

                    session.Store(new Dummy
                    {
                        Guid = Guid.NewGuid()
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<DummyIndex.Result, DummyIndex>()
                        .Where(x => x.Guid == newGuid)
                        .ToList();

                    Assert.Equal(1, list.Count);
                    Assert.Equal(newGuid, list[0].Guid);
                }
            }
        }

        [Fact]
        public void CanIndexGuidArrayProperly2()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DummyGuidList());

                var newGuid = Guid.NewGuid();
                using (var session = store.OpenSession())
                {
                    session.Store(new Dummy
                    {
                        Guid = Guid.NewGuid(),
                        List = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() },
                        Int = 1
                    });

                    session.Store(new Dummy
                    {
                        Guid = Guid.NewGuid(),
                        List = new List<Guid> { Guid.NewGuid(), newGuid },
                        Int = 2
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var list = session.Query<DummyGuidList.Result, DummyGuidList>()
                        .Where(x => x.DummyGuid.Any(g => g == newGuid))
                        .ToList();

                    Assert.Equal(1, list.Count);
                    Assert.Equal(1, list[0].DummyGuid.Length);
                    Assert.Equal(newGuid, list[0].DummyGuid[0]);
                }
            }
        }

        [Fact]
        public void CanIndexNumericArrayProperly()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DummyIndexCount());

                var newGuid = Guid.NewGuid();
                using (var session = store.OpenSession())
                {
                    session.Store(new Dummy
                    {
                        Guid = Guid.NewGuid(),
                        Int = 1,
                        Short = 1,
                        Byte = byte.MaxValue,
                        Float = 1f,
                        Decimal = 1,
                        Double = 1d
                    });

                    session.Store(new Dummy
                    {
                        Guid = newGuid,
                        Int = 2,
                        Short = 2,
                        Byte = byte.MaxValue,
                        Float = 1f,
                        Decimal = 1,
                        Double = 1d
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<DummyIndexCount.Result, DummyIndexCount>()
                        .Where(x => x.IntCount.Any(d => d == 2))
                        .ToList();

                    Assert.Equal(1, list.Count);
                    Assert.Equal(newGuid, list[0].Guid);
                    Assert.Equal(new int[] { 2 }, list[0].IntCount);
                    Assert.Equal(new short[] { 2 }, list[0].ShortCount);
                    Assert.Equal(new byte[] { byte.MaxValue }, list[0].ByteCount);
                    Assert.Equal(new float[] { 1 }, list[0].FloatCount);
                    Assert.Equal(new decimal[] { 1 }, list[0].DecimalCount);
                    Assert.Equal(new double[] { 1 }, list[0].DoubleCount);
                }
            }
        }

        private class DateIndex : AbstractIndexCreationTask<Date, DateIndex.Result>
        {
            public class Result
            {
                public DateTime DateTime { get; set; }
            }

            public DateIndex()
            {
                Map = dates =>
                    from date in dates
                    select new Result
                    {
                        DateTime = date.DateTime,
                    };
            }
        }

        private class LastAccessIndex : AbstractIndexCreationTask<User, LastAccessIndex.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public DateTime?[] LastAccess { get; set; }
            }

            public class LastAccess
            {
                public DateTime? DateTime { get; set; }
            }

            public LastAccessIndex()
            {
                Map = users =>
                    from user in users
                    where user.LoginsByDay.Count > 0
                    select new Result
                    {
                        Name = user.Name,
                        LastAccess = new DateTime?[]
                        {
                            user.LoginsByDay.LastOrDefault().Key
                        }
                    };
            }
        }

        private class LastAccessPerName : AbstractIndexCreationTask<User, LastAccessPerName.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public DateTime[] LastAccess { get; set; }
            }

            public class LastAccess
            {
                public DateTime? DateTime { get; set; }
            }

            public LastAccessPerName()
            {
                Map = users =>
                    from user in users
                    where user.LoginsByDay.Count > 0
                    select new Result
                    {
                        Name = user.Name,
                        LastAccess = new[]
                        {
                            user.LoginsByDay.LastOrDefault().Key
                        }
                    };

                Reduce = results =>
                    from result in results
                    group result by new { result.Name }
                    into g
                    select new Result
                    {
                        Name = g.Key.Name,
                        LastAccess = g
                            .OrderBy(x => x.LastAccess)
                            .Last().LastAccess
                    };
            }
        }

        private class DummyIndex : AbstractIndexCreationTask<Dummy, DummyIndex.Result>
        {
            public class Result
            {
                public Guid Guid { get; set; }
            }

            public DummyIndex()
            {
                Map = dummys =>
                    from dummy in dummys
                    select new Result
                    {
                        Guid = dummy.Guid
                    };
            }
        }

        private class DummyGuidList : AbstractIndexCreationTask<Dummy, DummyGuidList.Result>
        {
            public class Result
            {
                public Guid Guid { get; set; }

                public Guid[] DummyGuid { get; set; }
            }

            public class DummyCount
            {
                public Guid? Guid { get; set; }
            }

            public DummyGuidList()
            {
                Map = dummys =>
                    from dummy in dummys
                    select new Result
                    {
                        Guid = dummy.Guid,
                        DummyGuid = new[]
                        {
                            dummy.List.LastOrDefault()
                        }
                    };

                Reduce = results =>
                    from result in results
                    group result by new { result.Guid }
                    into g
                    select new Result
                    {
                        Guid = g.Key.Guid,
                        DummyGuid = g.Last().DummyGuid
                    };
            }
        }

        private class DummyIndexCount : AbstractIndexCreationTask<Dummy, DummyIndexCount.Result>
        {
            public class Result
            {
                public Guid Guid { get; set; }

                public int[] IntCount { get; set; }

                public short[] ShortCount { get; set; }

                public List<byte> ByteCount { get; set; }

                public float[] FloatCount { get; set; }

                public decimal[] DecimalCount { get; set; }

                public double[] DoubleCount { get; set; }
            }

            public DummyIndexCount()
            {
                Map = dummys =>
                    from dummy in dummys
                    select new Result
                    {
                        Guid = dummy.Guid,
                        IntCount = new[]
                        {
                            dummy.Int
                        },
                        ShortCount = new[]
                        {
                            dummy.Short
                        },
                        ByteCount = new List<byte>
                        {
                            dummy.Byte
                        },
                        FloatCount = new[]
                        {
                            dummy.Float
                        },
                        DecimalCount = new[]
                        {
                            dummy.Decimal
                        },
                        DoubleCount = new[]
                        {
                            dummy.Double
                        }
                    };

                Reduce = results =>
                    from result in results
                    group result by new { result.Guid }
                    into g
                    select new Result
                    {
                        Guid = g.Key.Guid,
                        IntCount = g.Last().IntCount,
                        ShortCount = g.Last().ShortCount,
                        FloatCount = g.Last().FloatCount,
                        ByteCount = g.Last().ByteCount,
                        DecimalCount = g.Last().DecimalCount,
                        DoubleCount = g.Last().DoubleCount
                    };
            }
        }

        private class Date
        {
            public DateTime DateTime { get; set; }
        }

        private class User
        {
            public User()
            {
                LoginsByDay = new Dictionary<DateTime, long>();
            }

            public string Name { get; set; }

            public Dictionary<DateTime, long> LoginsByDay { get; set; }
        }

        private class Dummy
        {
            public Guid Guid { get; set; }

            public List<Guid> List { get; set; }

            public int Int { get; set; }

            public short Short { get; set; }

            public byte Byte { get; set; }

            public float Float { get; set; }

            public decimal Decimal { get; set; }

            public double Double { get; set; }
        }
    }
}
