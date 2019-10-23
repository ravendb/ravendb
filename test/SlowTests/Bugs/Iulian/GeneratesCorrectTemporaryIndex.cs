using FastTests;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Iulian
{
    public class GeneratesCorrectTemporaryIndex : RavenTestBase
    {
        public GeneratesCorrectTemporaryIndex(ITestOutputHelper output) : base(output)
        {
        }

        private class Inner
        {
            public bool Flag { get; set; }
        }

        private class Outer
        {
            public Inner Inner { get; set; }
        }

        [Fact]
        public void Can_Generate_Correct_Temporary_Index()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    // store the element
                    Outer outer = new Outer { Inner = new Inner { Flag = true } };
                    s.Store(outer);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    // query by the inner flag
                    Outer outer = s.Query<Outer>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(o => o.Inner.Flag).SingleOrDefault();

                    Assert.NotNull(outer); // this fails
                }
            }
        }
    }
}
