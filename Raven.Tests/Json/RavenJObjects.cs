using Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Json
{
	public class RavenJObjects
	{
		[Fact]
		public void CanIgnoreUnassignedProperties()
		{
			var blogOne = new Blog
							{
								Title = "one",
								Category = "Ravens"
							};

			var o = RavenJObject.FromObject(blogOne);

			Assert.True(o.ContainsKey("Title")); // a property with a value
			Assert.Equal("one", o["Title"]);

			Assert.True(o.ContainsKey("User")); // a property we didn't assign
			Assert.True(o["User"].Type == JTokenType.Null);

			Assert.False(o.ContainsKey("foo")); // a non-existing property
			Assert.Null(o["foo"]);
		}

		public class Blog
		{
			public User User
			{
				get;
				set;
			}

			public string Title
			{
				get;
				set;
			}

			public Tag[] Tags
			{
				get;
				set;
			}

			public string Category
			{
				get;
				set;
			}
		}

		public class Tag
		{
			public string Name
			{
				get;
				set;
			}
		}

		public class User
		{
			public string Name
			{
				get;
				set;
			}
		}
	}
}
