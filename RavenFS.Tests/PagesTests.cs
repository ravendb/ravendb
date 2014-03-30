using System;
using System.Collections.Specialized;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;

using Xunit;

namespace RavenFS.Tests
{
	public class PagesTests : IDisposable
	{
		readonly TransactionalStorage storage;

		private readonly NameValueCollection metadataWithEtag = new NameValueCollection()
		                                               	{
		                                               		{"ETag", "\"" + Guid.Empty +"\""}
		                                               	};

		public PagesTests()
		{
			IOExtensions.DeleteDirectory("test");
			storage = new TransactionalStorage("test", metadataWithEtag);
			storage.Initialize();
		}

		[Fact]
		public void CanInsertPage()
		{
			storage.Batch(accessor => accessor.InsertPage(new byte[] { 1, 2, 3, 4, 5, 6 }, 4));
		}

		[Fact]
		public void CanAssociatePageWithFile()
		{
			storage.Batch(accessor =>
			{
				accessor.PutFile("test.csv", 12, metadataWithEtag);

				var hashKey = accessor.InsertPage(new byte[] {1, 2, 3, 4, 5, 6}, 4);
				accessor.AssociatePage("test.csv", hashKey,0, 4);

				hashKey = accessor.InsertPage(new byte[] {5, 6, 7, 8, 9}, 4);
				accessor.AssociatePage("test.csv", hashKey, 1, 4);
			});
		}

		[Fact]
		public void CanReadFilePages()
		{
			storage.Batch(accessor =>
			{
				accessor.PutFile("test.csv", 16, metadataWithEtag);

				var hashKey = accessor.InsertPage(new byte[] { 1, 2, 3, 4, 5, 6 }, 4);
				accessor.AssociatePage("test.csv", hashKey, 0, 4);

				hashKey = accessor.InsertPage(new byte[] { 5, 6, 7, 8, 9 }, 4);
				accessor.AssociatePage("test.csv", hashKey, 1, 4);

				hashKey = accessor.InsertPage(new byte[] { 6, 7, 8, 9 }, 4);
				accessor.AssociatePage("test.csv", hashKey, 2, 4);
			});


			storage.Batch(accessor =>
			{
				var file = accessor.GetFile("test.csv", 0, 2);
				Assert.NotNull(file);
				Assert.Equal(2, file.Pages.Count);
			});
		}

		[Fact]
		public void CanReadFilePages_SecondPage()
		{
			storage.Batch(accessor =>
			{
				accessor.PutFile("test.csv", 16, metadataWithEtag);

				var hashKey = accessor.InsertPage(new byte[] { 1, 2, 3, 4, 5, 6 }, 4);
				accessor.AssociatePage("test.csv", hashKey, 0, 4);

				hashKey = accessor.InsertPage(new byte[] { 5, 6, 7, 8, 9 }, 4);
				accessor.AssociatePage("test.csv", hashKey, 1, 4);

				hashKey = accessor.InsertPage(new byte[] { 6, 7, 8, 9 }, 4);
				accessor.AssociatePage("test.csv", hashKey, 2, 4);
			});


			storage.Batch(accessor =>
			{
				var file = accessor.GetFile("test.csv", 2, 2);
				Assert.NotNull(file);
				Assert.Equal(1, file.Pages.Count);
			});
		}

		[Fact]
		public void CanReadMetadata()
		{
			metadataWithEtag["test"] = "abc";
			storage.Batch(accessor => accessor.PutFile("test.csv", 16, metadataWithEtag));


			storage.Batch(accessor =>
			{
				var file = accessor.GetFile("test.csv", 2, 2);
				Assert.NotNull(file);
				Assert.Equal("abc", file.Metadata["test"]);
			});
		}

		[Fact]
		public void CanReadFileContents()
		{
			storage.Batch(accessor =>
			{
				accessor.PutFile("test.csv", 16, metadataWithEtag);

				var hashKey = accessor.InsertPage(new byte[] { 1, 2, 3, 4, 5, 6 }, 4);
				accessor.AssociatePage("test.csv", hashKey, 0, 4);

				hashKey = accessor.InsertPage(new byte[] { 5, 6, 7, 8, 9 }, 4);
				accessor.AssociatePage("test.csv", hashKey, 1, 4);

				hashKey = accessor.InsertPage(new byte[] { 6, 7, 8, 9 }, 4);
				accessor.AssociatePage("test.csv", hashKey, 2, 4);
			});


			storage.Batch(accessor =>
			{
				var file = accessor.GetFile("test.csv", 0, 4);
				var bytes = new byte[4];

				accessor.ReadPage(file.Pages[0].Id, bytes);
				Assert.Equal(new byte[] { 1, 2, 3, 4 }, bytes);

				accessor.ReadPage(file.Pages[1].Id, bytes);
				Assert.Equal(new byte[] {5, 6, 7, 8}, bytes);
				accessor.ReadPage(file.Pages[2].Id, bytes);
				Assert.Equal(new byte[] {6, 7, 8, 9}, bytes);
			});
		}

		[Fact]
		public void CanInsertAndReadPage()
		{
			var key = 0;
			storage.Batch(accessor =>
			{
				key = accessor.InsertPage(new byte[] { 1, 2, 3, 4, 5, 6 }, 4);
			});

			storage.Batch(accessor =>
			{
				var buffer = new byte[4];
				Assert.Equal(4, accessor.ReadPage(key, buffer));
				Assert.Equal(new byte[] { 1, 2, 3, 4 }, buffer);
			});
		}

		public void Dispose()
		{
			storage.Dispose();
		}
	}
}
