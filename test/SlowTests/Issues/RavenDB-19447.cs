using System;
using System.Linq;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Documents.Session.TimeSeries;

namespace SlowTests.Issues
{
    public class RavenDB_19447 : RavenTestBase
    {
        public RavenDB_19447(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public void TesCase1(bool justCreate, bool register)
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                //if justCreate is true - only create entity in the timeseries
                // else - create and then increment
                int mul = 2;
                if (justCreate)
                {
                    mul = 1;
                }

                if(register)
                    store.TimeSeries.Register<User, TestObj>("INC:Heartrate");

                for (int i = 0; i < mul; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.IncrementalTimeSeriesFor<TestObj>("users/ayende", "INC:Heartrate")
                            .Increment(baseline.AddMinutes(2), new TestObj
                            {
                                A = 1.1,
                                B = 2.2,
                                C = 3.3
                            });

                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    var val = session.IncrementalTimeSeriesFor("users/ayende", "INC:Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(1, val.Count);

                    var values = val[0].Values;
                    Assert.Equal(3, values.Length);
                    Assert.Equal(1.1 * mul, values[0]);
                    Assert.Equal(2.2 * mul, values[1]);
                    Assert.Equal(3.3 * mul, values[2]);
                    
                }

                using (var session = store.OpenSession())
                {
                    var val = session.IncrementalTimeSeriesFor<TestObj>("users/ayende", "INC:Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(1, val.Count);

                    var entity = val[0].Value;
                    Assert.NotNull(entity);
                    Assert.Equal(1.1 * mul, entity.A);
                    Assert.Equal(2.2 * mul, entity.B);
                    Assert.Equal(3.3 * mul, entity.C);

                }
            }
        }

        public class TestObj
        {
            [TimeSeriesValue(0)]
            public double A { get; set; }

            [TimeSeriesValue(1)]
            public double B { get; set; }

            [TimeSeriesValue(2)]
            public double C { get; set; }
        }
    }
}
