using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Blittable
{
    public class Serialization : RavenTestBase
    {
        public Serialization(ITestOutputHelper output) : base(output)
        {
        }

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

        [Fact]
        public void Can_Store_And_Load_Nullable()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new NullableDummy() { NullInt = 1, NullBool = true, NullDouble = double.MinValue, NullFloat = float.MaxValue}, "dummies/1");
                    session.Store(new NullableDummy() { NullInt = null, NullBool = null, NullDouble = null, NullFloat = null }, "dummies/2");

                    session.SaveChanges();

                    var doc1 = session.Load<NullableDummy>("dummies/1");
                    var doc2 = session.Load<NullableDummy>("dummies/2");

                    Assert.Equal(doc1.NullInt ,1);
                    Assert.True(doc1.NullBool);
                    Assert.Equal(doc1.NullDouble, double.MinValue);
                    Assert.Equal(doc1.NullFloat, float.MaxValue);

                    Assert.Null(doc2.NullInt);
                    Assert.Null(doc2.NullBool);
                    Assert.Null(doc2.NullDouble);
                    Assert.Null(doc2.NullFloat);
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

        public class NullableDummy
        {
            public int? NullInt { get; set; }
            public bool? NullBool { get; set; }
            public float? NullFloat { get; set; }
            public double? NullDouble { get; set; }
        }
    }
}
