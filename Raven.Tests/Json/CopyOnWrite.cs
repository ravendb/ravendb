using System.Collections.Generic;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Json
{
    public class CopyOnWrite
    {
		[Fact]
		public void CopyOnWriteStoresValues()
		{
			var f = new RavenJObject();
			f.Add("test", "Test");
			f.Add("2nd", "second");
			Assert.True(f.Properties.Values.Count == 2);
		}

        [Fact]
        public void CopyOnWriteWorksCorrectly()
        {
            var f = new RavenJObject();
            f.Properties["1"] = new RavenJValue(1);
            f.Properties["2"] = new RavenJValue(2);

            var f1 = (RavenJObject)f.CloneToken();
            f1.Properties["2"] = new RavenJValue(3);

            var val = (RavenJValue) f.Properties["2"];
            Assert.Equal(2, val.Value);
            val = (RavenJValue)f1.Properties["2"];
            Assert.Equal(3, val.Value);

            var f2 = (RavenJObject)f1.CloneToken();
            val = (RavenJValue)f2.Properties["2"];
            Assert.Equal(3, val.Value);

            f.Properties["2"] = f2;
            f1 = (RavenJObject) f.CloneToken();
            f.Properties.Remove("2");
        	Assert.Throws(typeof (KeyNotFoundException), () => f.Properties["2"]);
            Assert.NotNull(f1.Properties["2"]);
        }

		[Fact]
		public void ChangingValuesOfParent()
		{
			var obj = RavenJObject.Parse(" { 'User': { 'Name': 'Ayende'}  }");
			var obj2 = obj.CloneToken();
			var obj3 = obj.CloneToken();
			obj2.Value<RavenJObject>("User")["Name"] = "Rahien";
			obj3.Value<RavenJObject>("User")["Name"] = "Oren";
			Assert.Equal("Ayende", obj.Value<RavenJObject>("User").Value<string>("Name"));
			Assert.Equal("Rahien", obj2.Value<RavenJObject>("User").Value<string>("Name"));
			Assert.Equal("Oren", obj3.Value<RavenJObject>("User").Value<string>("Name"));
		}

		[Fact]
		public void ShouldNotFail()
		{
			var root = new RavenJObject();
			var current = root;
			for (int i = 0; i < 10000; i++)
			{
				var temp = new RavenJObject();
				current.Add("Inner", temp);
				current = temp;
			}

			var anotherRoot = root.CloneToken() as RavenJObject;

			do
			{
				anotherRoot["TestProp"] = 0;
			} while ((anotherRoot = anotherRoot["Inner"] as RavenJObject) != null);
		}
    }
}
