using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Json
{
	public class CloningTests
	{


		public class Blog
		{
			public string Title
			{
				get;
				set;
			}

			public string Category
			{
				get;
				set;
			}

			public BlogTag[] Tags
			{
				get;
				set;
			}
		}

		public class BlogTag
		{
			public string Name { get; set; }
		}

		[Fact]
		public void WhenCloningWillRetainAllValues()
		{
			var newBlog = new Blog()
			{
				Tags = new[]{
			          new BlogTag() { Name = "SuperCallaFragalisticExpealadocious" }
			     }
			};

			var expected = RavenJObject.FromObject(newBlog);
			var actual = new RavenJObject(expected);

			Assert.Equal(expected.ToString(Formatting.None), actual.ToString(Formatting.None));
		}

		[Fact]
		public void CloningTestsStoresValues()
		{
			var f = new RavenJObject();
			f.Add("test", "Test");
			f.Add("2nd", "second");
			Assert.True(f.Count == 2);
		}

		[Fact]
		public void CloningTestsWorksCorrectly()
		{
			var f = new RavenJObject();
			f["1"] = new RavenJValue(1);
			f["2"] = new RavenJValue(2);

			var f1 = (RavenJObject)f.CloneToken();
			f1["2"] = new RavenJValue(3);

			var val = (RavenJValue) f["2"];
			Assert.Equal(2, val.Value);
			val = (RavenJValue)f1["2"];
			Assert.Equal(3, val.Value);

			var f2 = (RavenJObject)f1.CloneToken();
			val = (RavenJValue)f2["2"];
			Assert.Equal(3, val.Value);

			f["2"] = f2;
			f1 = (RavenJObject) f.CloneToken();
			f.Remove("2");
			Assert.Null(f["2"]);
			Assert.NotNull(f1["2"]);
		}

		[Fact]
		public void ChangingValuesOfParent()
		{
			var obj = RavenJObject.Parse(" { 'Me': { 'ObjectID': 1}  }");
			var obj2 = obj.CloneToken();
			var obj3 = obj.CloneToken();

			var o = obj2.Value<RavenJObject>("Me");
			o["ObjectID"] = 2;

			obj3.Value<RavenJObject>("Me")["ObjectID"] = 3;
			Assert.Equal(1, obj.Value<RavenJObject>("Me").Value<int>("ObjectID"));
			Assert.Equal(2, obj2.Value<RavenJObject>("Me").Value<int>("ObjectID"));
			Assert.Equal(3, obj3.Value<RavenJObject>("Me").Value<int>("ObjectID"));
		}

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

			var anotherRoot = (RavenJObject)root.CloneToken();
			do
			{
				anotherRoot["Inner"] = 0;
			} while ((anotherRoot = anotherRoot["Inner"] as RavenJObject) != null);
		}

		[Fact]
		public void ShouldBehaveNicelyInMultithreaded()
		{
			var obj = new RavenJObject
			          	{
			          		{"prop1", 2},
			          		{"prop2", "123"}
			          	};

			var copy = (RavenJObject)obj.CloneToken() ;
			copy["@id"] = "movies/1";

			Parallel.For(0, 10000, i =>
			                       	{
			                       		Assert.True(copy.ContainsKey("@id"));
			                       		var foo = (RavenJObject)copy.CloneToken();
			                       		Assert.True(foo.ContainsKey("@id"));
			                       		Assert.True(copy.ContainsKey("@id"));
			                       	});
		}
	}
}
