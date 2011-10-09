
namespace Raven.Tests.Bugs.Iulian
{
    using System.Linq;
    using Xunit;

    /// <summary>
    /// Test that verifies that the generated index is correct.
    /// The following index is generated:
    /// Name: Temp/Outers/ByFlag
    /// Map: from doc in docs.Outers select new { Flag = doc.Flag }
    /// 
    /// The map part is wrong since the projection should be new { Flag = doc.Inner.Flag }
    /// 
    /// </summary>
    public class GeneratesCorrectTemporaryIndex : LocalClientTest
    {
        public class Inner
        {
            public bool Flag { get; set; }
        }

        public class Outer
        {
            public Inner Inner { get; set; }
        }

        [Fact]
        public void Can_Generate_Correct_Temporary_Index()
        {
            using (var store = base.NewDocumentStore().Initialize())
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
                    // verify that the element is saved as expected ( this passes )
                    Outer test = s.Query<Outer>().SingleOrDefault();
                    Assert.NotNull(test);
                    Assert.True(test.Inner.Flag);

                    // query by the inner flag
                    Outer outer = s.Query<Outer>().Where(o => o.Inner.Flag).SingleOrDefault();

                    Assert.NotNull(outer); // this fails
                }
            }
        }
    }
}
