using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Linq;
using Raven.Client.Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class markp : RavenTestBase
    {
        public markp(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name;
        }

        [Fact]
        public void CanQueryUsingInWhenUsingCustomSerialization()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new JsonNetSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.PreserveReferencesHandling = PreserveReferencesHandling.All;
                        }
                    };
                }
            }))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Name = "Oren"
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var r = s.Query<User>().Where(x => x.Name.In("Oren")).ToList();
                    Assert.NotEmpty(r);
                }
            }
        }
    }
}
