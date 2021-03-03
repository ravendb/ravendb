using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class CanParseToObjectFromBlittable : NoDisposalNeeded
    {
        public CanParseToObjectFromBlittable(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
#pragma warning disable 649
            public string Id;
            public string Name;
            public int Age;
            public string[] Dogs;
            public List<Child> Children;
#pragma warning restore 649
        }

        private class Child
        {
#pragma warning disable 649
            public string Name;
#pragma warning restore 649
        }

        [Fact]
        public void Directly()
        {
            var json = "{'Id':'users/1', 'Name': 'Oren', 'Dogs':['Arava','Oscar','Phoebe'], 'Age': 34, 'Children':[{'Name':'Date'}]}";

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                BlittableJsonReaderObject reader = ctx.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(json)), "users/1");

                var serializer = new JsonSerializer();
                var blittableJsonReader = new BlittableJsonReader();
                blittableJsonReader.Initialize(reader);
                var u = serializer.Deserialize<User>(blittableJsonReader);

                Assert.Equal("Oren", u.Name);
                Assert.Equal("users/1", u.Id);
                Assert.Equal(3, u.Dogs.Length);
                Assert.Equal("Arava", u.Dogs[0]);
                Assert.Equal("Oscar", u.Dogs[1]);
                Assert.Equal("Phoebe", u.Dogs[2]);
                Assert.Equal(34, u.Age);

                Assert.Equal(1, u.Children.Count);
                Assert.Equal("Date", u.Children[0].Name);
            }
        }
    }
}
