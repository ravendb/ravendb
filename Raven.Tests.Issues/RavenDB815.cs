using System.Collections.Generic;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB815 : RavenTestBase
	{
		public class Foo1
		{
			public string Id { get; set; }
			public IList<string> Strings { get; set; }
		}

		public class Foo2
		{
			public string Id { get; set; }
			public IEnumerable<string> Strings { get; set; }
		}

		public class Foo3
		{
			public string Id { get; set; }
			public List<string> Strings { get; set; }
		}

		public class Foo4
		{
			public string Id { get; set; }
			public string[] Strings { get; set; }
		}

		public class Foo5
		{
			public string Id { get; set; }
			public IList<Bar> Bars { get; set; }
		}

		public class Foo6
		{
			public string Id { get; set; }
			public IEnumerable<Bar> Bars { get; set; }
		}

		public class Foo7
		{
			public string Id { get; set; }
			public List<Bar> Bars { get; set; }
		}

		public class Foo8
		{
			public string Id { get; set; }
			public Bar[] Bars { get; set; }
		}

		public class Bar
		{
			public string Baz { get; set; }
		}

		[Fact]
		public void Inner_IList_Strings_From_Array()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo1 { Id = "foos/1", Strings = new[] { "A", "B", "C" } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Strings"": [
    ""A"",
    ""B"",
    ""C""
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_IList_Strings_From_List()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo1 { Id = "foos/1", Strings = new List<string> { "A", "B", "C" } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Strings"": [
    ""A"",
    ""B"",
    ""C""
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_IEnumerable_Strings_From_Array()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo2 { Id = "foos/1", Strings = new[] { "A", "B", "C" } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Strings"": [
    ""A"",
    ""B"",
    ""C""
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_IEnumerable_Strings_From_List()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo2 { Id = "foos/1", Strings = new List<string> { "A", "B", "C" } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Strings"": [
    ""A"",
    ""B"",
    ""C""
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_List_Strings()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo3 { Id = "foos/1", Strings = new List<string> { "A", "B", "C" } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Strings"": [
    ""A"",
    ""B"",
    ""C""
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_Array_Strings()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo4 { Id = "foos/1", Strings = new[] { "A", "B", "C" } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Strings"": [
    ""A"",
    ""B"",
    ""C""
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_IList_Objects_From_Array()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo5 { Id = "foos/1", Bars = new[] { new Bar { Baz = "A" }, new Bar { Baz = "B" }, new Bar { Baz = "C" } } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Bars"": [
    {
      ""Baz"": ""A""
    },
    {
      ""Baz"": ""B""
    },
    {
      ""Baz"": ""C""
    }
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_IList_Objects_From_List()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo5 { Id = "foos/1", Bars = new List<Bar> { new Bar { Baz = "A" }, new Bar { Baz = "B" }, new Bar { Baz = "C" } } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Bars"": [
    {
      ""Baz"": ""A""
    },
    {
      ""Baz"": ""B""
    },
    {
      ""Baz"": ""C""
    }
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_IEnumerable_Objects_From_Array()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo6 { Id = "foos/1", Bars = new[] { new Bar { Baz = "A" }, new Bar { Baz = "B" }, new Bar { Baz = "C" } } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Bars"": [
    {
      ""Baz"": ""A""
    },
    {
      ""Baz"": ""B""
    },
    {
      ""Baz"": ""C""
    }
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_IEnumerable_Objects_From_List()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo6 { Id = "foos/1", Bars = new List<Bar> { new Bar { Baz = "A" }, new Bar { Baz = "B" }, new Bar { Baz = "C" } } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Bars"": [
    {
      ""Baz"": ""A""
    },
    {
      ""Baz"": ""B""
    },
    {
      ""Baz"": ""C""
    }
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_List_Objects()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo7 { Id = "foos/1", Bars = new List<Bar> { new Bar { Baz = "A" }, new Bar { Baz = "B" }, new Bar { Baz = "C" } } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Bars"": [
    {
      ""Baz"": ""A""
    },
    {
      ""Baz"": ""B""
    },
    {
      ""Baz"": ""C""
    }
  ]
}", doc);
			}
		}

		[Fact]
		public void Inner_Array_Objects()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo8 { Id = "foos/1", Bars = new[] { new Bar { Baz = "A" }, new Bar { Baz = "B" }, new Bar { Baz = "C" } } });
					session.SaveChanges();
				}

				var doc = documentStore.DatabaseCommands.Get("foos/1").DataAsJson.ToString();
				Assert.Equal(@"{
  ""Bars"": [
    {
      ""Baz"": ""A""
    },
    {
      ""Baz"": ""B""
    },
    {
      ""Baz"": ""C""
    }
  ]
}", doc);
			}
		}
	}
}