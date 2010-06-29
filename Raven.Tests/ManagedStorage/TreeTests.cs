using System.IO;
using Newtonsoft.Json.Linq;
using Raven.Storage.Managed.Data;
using Xunit;

namespace Raven.Storage.Tests
{
	public class TreeTests
	{
		[Fact]
		public void CanAddToTree()
		{
			var buffer = new byte[1024];
			var tree = new Tree(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			tree.Add("ayende", 13);

			var docPos = tree.FindValue("ayende");
			Assert.Equal(13, docPos);
		}

		[Fact(Skip = "The impl is broken :-(")]
		public void CanAddAndSearch()
		{
			var buffer = new byte[1024];
			var tree = new Tree(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			for (int i = 0; i < 7; i++)
			{
				tree.Add("commitinfos/" + (i + 1), i);
			}

			for (int i = 0; i < 7; i++)
			{
				Assert.NotNull(tree.FindNode("commitinfos/" + (i + 1)));
			}
		}

		[Fact]
		public void PartialSearches_ShouldResultInIndexScan()
		{
			var buffer = new byte[1024];
			var tree = new Tree(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			tree.Add(JObject.FromObject(new { One = 1, Two = 2 }), 13);
			tree.Add(JObject.FromObject(new { One = 3, Two = 3 }), 14);
			tree.Add(JObject.FromObject(new { One = 3, Two = 1 }), 15);

			var docPos = tree.FindValue(JObject.FromObject(new { One = 1, Two = 2 }));
			Assert.Equal(13, docPos);

			docPos = tree.FindValue(JObject.FromObject(new { Two = 2 }));
			Assert.Equal(13, docPos);

			docPos = tree.FindValue(JObject.FromObject(new { Two = 3 }));
			Assert.Equal(14, docPos);

			docPos = tree.FindValue(JObject.FromObject(new { Two = 1 }));
			Assert.Equal(15, docPos);
		}

		[Fact]
		public void CanAddToTreeAndReadFromAnother()
		{
			var buffer = new byte[1024];
			var tree = new Tree(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			tree.Add("ayende", 45);
			tree.Flush();

			tree = new Tree(new MemoryStream(buffer)
			{
				Position = tree.RootPosition
			}, new MemoryStream(buffer), StartMode.Open);
			var doc = tree.FindValue("ayende");
			Assert.Equal(45, doc);
		}

		[Fact]
		public void CanSearchInTree()
		{
			var buffer = new byte[1024];
			var tree = new Tree(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			tree.Add("ayende", 53);
			tree.Add("oren", 15);

			var doc = tree.FindValue("oren");
			Assert.Equal(15, doc);
		}

		[Fact]
		public void CanSearchInTreeAndReadFromAnother()
		{
			var buffer = new byte[1024];
			var tree = new Tree(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			tree.Add("ayende", 15);
			tree.Add("oren", 13);
			tree.Flush();

			tree = new Tree(new MemoryStream(buffer)
			{
				Position = tree.RootPosition
			}, new MemoryStream(buffer), StartMode.Open);
			var doc = tree.FindValue("oren");
			Assert.Equal(13, doc);
		}

		[Fact]
		public void CanRemoveFromTree()
		{
			var buffer = new byte[1024];
			var tree = new Tree(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			tree.Add("ayende", 44);
			tree.Add("oren", 77);

			var doc = tree.FindValue("oren");
			Assert.Equal(77, doc);

			tree.Remove("ayende");

			Assert.Null(tree.FindValue("ayende"));
			doc = tree.FindValue("oren");
			Assert.Equal(77, doc);

		}
		[Fact]
		public void CanRemoveFromTreeAndReadFromAnother()
		{
			var buffer = new byte[1024];
			var tree = new Tree(new MemoryStream(buffer), new MemoryStream(buffer), StartMode.Create);
			tree.Add("ayende", 13);
			tree.Add("oren", 978);
			tree.Remove("ayende");
			tree.Flush();

			tree = new Tree(new MemoryStream(buffer)
			{
				Position = tree.RootPosition
			}, new MemoryStream(buffer), StartMode.Open);
			Assert.Null(tree.FindValue("ayende"));
			var doc = tree.FindValue("oren");
			Assert.Equal(978, doc);
		}
	}
}