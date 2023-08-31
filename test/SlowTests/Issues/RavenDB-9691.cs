using FastTests;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9691 : RavenTestBase
    {
        public RavenDB_9691(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_serialize_with_default_enum_value()
        {
            var user = new Document
            {
                Name = "john"
            };
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var blittable = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(user, context);
                Assert.Equal("{\"Name\":\"john\",\"Type\":0}", blittable.ToString());
            }
        }

        public enum DocumentType
        {
            Foo = 1
        }


        public class Document
        {
            public string Name { get; set; }
            public DocumentType Type { get; set; }
        }
    }
}
