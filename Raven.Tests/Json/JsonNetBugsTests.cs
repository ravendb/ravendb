
namespace Raven.Tests.Json
{
    using System.Collections.Concurrent;
    using Xunit;

    public class JsonNetBugsTests : LocalClientTest
    {
        class ObjectyWithByteArray
        {
            public byte[] Data {get;set;}
        }

        [Fact]
        public void can_serialize_object_whth_byte_array_when_TypeNameHandling_is_All()
        {
            ObjectyWithByteArray data = new ObjectyWithByteArray { Data = new byte[] { 72, 63, 62, 71, 92, 55 } };
            using (var store = NewDocumentStore())
            {
                // this is an edge case since it does not make a lot of sense for users to set this.
                store.Conventions.CustomizeJsonSerializer = x => x.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.All;

                using (var session = store.OpenSession())
                {
                    session.Store(data, "test");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Load<ObjectyWithByteArray>("test");
                    Assert.NotNull(result);
                    Assert.Equal(data.Data, result.Data);   
                }
            }
        }

        class ObjectWithConcurentDictionary
        {
            public ConcurrentDictionary<string, string> Data { get; set; }
        }

        [Fact]
        public void cal_serialize_object_with_concurentdictionary()
        {
            ObjectWithConcurentDictionary data = new ObjectWithConcurentDictionary
            {
                Data = new ConcurrentDictionary<string, string>()
            };

            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(data, "test");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Load<ObjectWithConcurentDictionary>("test");
                    Assert.NotNull(result);
                }
            }
        }

    }
}
