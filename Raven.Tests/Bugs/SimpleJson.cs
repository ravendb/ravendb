using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class SimpleJson : RavenTest
	{
		[Fact]
		public void ShouldNotGenerateComplexJsonForDefaultValues_Array()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new TestItem
					{
						Tags = new[]{"item", "two", "four"}
					});
					session.SaveChanges();
				}

				var doc = store.DatabaseCommands.Get("testitems/1").DataAsJson.ToString(Formatting.None);
				Assert.Equal("{\"Tags\":[\"item\",\"two\",\"four\"],\"Attributes\":null}", doc);
			}
		}

		[Fact]
		public void ShouldNotGenerateComplexJsonForDefaultValues_List()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestItem
					{
						Tags = new[] { "item", "two", "four" }.ToList()
					});
					session.SaveChanges();
				}

				var doc = store.DatabaseCommands.Get("testitems/1").DataAsJson.ToString(Formatting.None);
				Assert.Equal("{\"Tags\":[\"item\",\"two\",\"four\"],\"Attributes\":null}", doc);
			}
		}

		[Fact]
		public void ShouldNotGenerateComplexJsonForDefaultValues_Dictionary()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestItem
					{
						Attributes = new Dictionary<string, string>
						{
							{"User","Ayende"}
						}
					});
					session.SaveChanges();
				}

				var doc = store.DatabaseCommands.Get("testitems/1").DataAsJson.ToString(Formatting.None);
				Assert.Equal("{\"Tags\":null,\"Attributes\":{\"User\":\"Ayende\"}}", doc);
			}
		}

		public class TestItem
		{
			public ICollection<string> Tags { get; set; }
			public IDictionary<string, string> Attributes { get; set; }
		}
	}
}