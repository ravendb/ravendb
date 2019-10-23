using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class Arrays : RavenTestBase
    {
        public Arrays(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanRetrieveMultiDimensionalArray()
        {
            using (var store = GetDocumentStore())
            {
                var arrayValue = new double[,] 
                {
                    { 1, 2 }, 
                    { 3, 4 }, 
                    { 5, 6 } 
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new ArrayHolder
                        {
                            ArrayValue = arrayValue
                        });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var arrayHolder = session.Load<ArrayHolder>("arrayholders/1-A");

                    Assert.Equal(arrayValue, arrayHolder.ArrayValue);
                }
            }
        }

        private class ArrayHolder
        {
            public double[,] ArrayValue { get; set; }
        }
    }
}
