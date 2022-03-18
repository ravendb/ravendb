using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class NumericIndexingTest : RavenTestBase
    {
        public NumericIndexingTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DoubleIndexing()
        {
            using (var store = GetDocumentStore())
            {
                new DoubleIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new TestDouble { Id = "Docs/1", DoubleValue = 1 });
                    session.Store(new TestDouble { Id = "Docs/2", DoubleValue = double.NaN });
                    session.Store(new TestDouble { Id = "Docs/3", DoubleValue = double.PositiveInfinity });
                    session.Store(new TestDouble { Id = "Docs/4", DoubleValue = double.NegativeInfinity });
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<TestDoubleView, DoubleIndex>()
                        .ProjectInto<TestDoubleView>()
                        .ToArray();

                    Assert.Equal(1, results[0].DoubleValue);
                    Assert.Equal(double.NaN, results[1].DoubleValue);
                    Assert.Equal(double.PositiveInfinity, results[2].DoubleValue);
                    Assert.Equal(double.NegativeInfinity, results[3].DoubleValue);
                    Assert.Equal(4, results.Length);
                }
            }
        }

        [Fact]
        public void FloatIndexing()
        {
            using (var store = GetDocumentStore())
            {
                new FloatIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new TestFloat { Id = "Docs/1", FloatValue = float.NaN });
                    session.Store(new TestFloat { Id = "Docs/2", FloatValue = float.PositiveInfinity });
                    session.Store(new TestFloat { Id = "Docs/3", FloatValue = float.NegativeInfinity });
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<TestFloatView, FloatIndex>()
                        .ProjectInto<TestFloatView>()
                        .ToArray();

                    Assert.Equal(float.NaN, results[0].FloatValue);
                    Assert.Equal(float.PositiveInfinity, results[1].FloatValue);
                    Assert.Equal(float.NegativeInfinity, results[2].FloatValue);
                    Assert.Equal(3, results.Length);
                }
            }
        }

        [Fact]
        public void IndexingAFloatField()
        {
            using (var store = GetDocumentStore())
            {
                new FloatIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new TestFloat { Id = "Docs/1", FloatValue = 1 });
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<TestFloatView, FloatIndex>()
                        .ProjectInto<TestFloatView>()
                        .ToArray();

                    Assert.Equal(1, results[0].FloatValue);
                    Assert.Equal(1, results.Length);
                }
            }
        }
    }

    public class DoubleIndex : AbstractIndexCreationTask<TestDouble, TestDoubleView>
    {
        public DoubleIndex()
        {
            Map = docs => from doc in docs
                          select new TestDoubleView
                          {
                              Id = doc.Id,
                              DoubleValue = !double.IsNaN((double)(doc.DoubleValue)) && !double.IsInfinity((double)(doc.DoubleValue))
                                  ? doc.DoubleValue
                                  : (double?)null,
                          };
        }
    }

    public class TestDouble
    {
        public string Id { get; set; }
        public double DoubleValue { get; set; }
    }

    public class TestDoubleView
    {
        public string Id { get; set; }
        public double? DoubleValue { get; set; }
    }

    public class FloatIndex : AbstractIndexCreationTask<TestFloat, TestFloatView>
    {
        public FloatIndex()
        {
            Map = docs => from doc in docs
                          select new TestFloatView
                          {
                              Id = doc.Id,
                              FloatValue = !float.IsNaN((float)(doc.FloatValue)) && !float.IsInfinity((float)(doc.FloatValue))
                                  ? doc.FloatValue
                                  : (float?)null,
                          };
        }
    }

    public class TestFloat
    {
        public string Id { get; set; }
        public float FloatValue { get; set; }
    }

    public class TestFloatView
    {
        public string Id { get; set; }
        public float? FloatValue { get; set; }
    }
}
