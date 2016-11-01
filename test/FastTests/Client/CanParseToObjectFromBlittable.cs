using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Client.Json;
using Raven.Imports.Newtonsoft.Json;
using Sparrow.Json;
using Xunit;

namespace FastTests.Client
{
    public class CanParseToObjectFromBlittable : NoDisposalNeeded
    {
        public class User
        {
            public string Id;
            public string Name;
            public int Age;
            public string[] Dogs;
            public List<Child> Children;
        }
        public class Child
        {
            public string Name;
        }

        [Fact]
        public void Directly()
        {
            var json = "{'Id':'users/1', 'Name': 'Oren', 'Dogs':['Arava','Oscar','Phoebe'], 'Age': 34, 'Children':[{'Name':'Date'}]}";

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                BlittableJsonReaderObject reader = ctx.Read(new MemoryStream(Encoding.UTF8.GetBytes(json)), "users/1");

                var serializer = new JsonSerializer();
                var u = serializer.Deserialize<User>(new BlittableJsonReader(reader));

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