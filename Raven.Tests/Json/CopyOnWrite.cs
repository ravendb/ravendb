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
    }
}
