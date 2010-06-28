using Newtonsoft.Json.Linq;
using Raven.Storage.Managed;
using Xunit;

namespace Raven.Storage.Tests
{
	public class Documents : TxStorageTest
	{
		[Fact]
		public void CanAddAndRead()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));

				JObject document = null;
				tx.Read(viewer =>
				{
					document = viewer.Documents.DocumentByKey("Ayende", null).DataAsJson;
				});

				Assert.Equal("Rahien", document.Value<string>("Name"));
			}
		}


		[Fact]
		public void CanAddAndReadFileAfterReopen()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
			}

			using (var tx = new TransactionalStorage("test"))
			{
				JObject document = null;
				tx.Read(viewer =>
				{
					document = viewer.Documents.DocumentByKey("Ayende", null).DataAsJson;
				});

				Assert.Equal("Rahien", document.Value<string>("Name"));
			}
		}

		[Fact]
		public void CanDeleteFile()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
				JObject metadata;
				tx.Write(mutator => mutator.Documents.DeleteDocument("Ayende", null, out metadata));
			}

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Read(viewer => Assert.Null(viewer.Documents.DocumentByKey("Ayende", null)));

			}
		}

		[Fact]
		public void CanCountDocuments()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
				tx.Read(accessor => Assert.Equal(1, accessor.Documents.GetDocumentsCount()));
				JObject metadata;
				tx.Write(mutator => mutator.Documents.DeleteDocument("Ayende", null, out metadata));

				tx.Read(accessor => Assert.Equal(0, accessor.Documents.GetDocumentsCount()));

			}

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Read(viewer => Assert.Null(viewer.Documents.DocumentByKey("Ayende", null)));
				tx.Read(accessor => Assert.Equal(0, accessor.Documents.GetDocumentsCount()));
			}
		}
	}
}