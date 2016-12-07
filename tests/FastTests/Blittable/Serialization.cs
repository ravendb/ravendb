using Xunit;

namespace NewClientTests.NewClient.Blittable
{
    public class Serialization : RavenTestBase
    {
        [Fact]
        public void Can_Store_And_Load_Boolean_And_Nullable_Boolean()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dummy() { EntityName = "Dumb", LuckyNumber = 13, IsLucky = true, IsRich = true }, "dummies/1");
                    session.Store(new Dummy() { EntityName = "Dumb & Dumber", LuckyNumber = 666, IsLucky = false, IsRich = null }, "dummies/2");

                    session.SaveChanges();

                    var doc1 = session.Load<Dummy>("dummies/1");
                    var doc2 = session.Load<Dummy>("dummies/2");

                    Assert.True(doc1.IsLucky);
                    Assert.True(doc1.IsRich);
                    Assert.False(doc2.IsLucky);
                    Assert.Null(doc2.IsRich);
                }
            }
        }

        public class Dummy
        {
            public string EntityName { get; set; }
            public int LuckyNumber { get; set; }
            public bool IsLucky { get; set; }
            public bool? IsRich { get; set; }
        }
    }
}