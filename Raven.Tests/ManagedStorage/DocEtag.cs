using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Storage.Managed;
using Xunit;

namespace Raven.Storage.Tests
{
	public class DocEtag : TxStorageTest
	{

		[Fact]
		public void EtagsAreAlwaysIncreasing()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator =>
				{
					mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject());
					mutator.Documents.AddDocument("Oren", null, JObject.FromObject(new { Name = "Eini" }), new JObject());
				});
			}

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Read(viewer =>
				{
					var doc1 = viewer.Documents.DocumentByKey("Ayende", null);
					var doc2 = viewer.Documents.DocumentByKey("Oren", null);
					Assert.Equal(1, doc2.Etag.CompareTo(doc1.Etag));

				});
			}

		}

		[Fact]
		public void CanGetDocumentByEtag()
		{

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator =>
				{
					mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject());
					mutator.Documents.AddDocument("Oren", null, JObject.FromObject(new { Name = "Eini" }), new JObject());
				});
			}

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Read(viewer =>
				{
					Assert.Equal(2, viewer.Documents.GetDocumentsAfter(Guid.Empty).Count());
					var doc1 = viewer.Documents.DocumentByKey("Ayende", null);
					Assert.Equal(1, viewer.Documents.GetDocumentsAfter(doc1.Etag).Count());
				});
			}
		}

		[Fact]
		public void CanGetDocumentByUpdateOrder()
		{

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator =>
				{
					mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject());
					mutator.Documents.AddDocument("Oren", null, JObject.FromObject(new { Name = "Eini" }), new JObject());
				});
			}

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Read(viewer =>
				{
					Assert.Equal(2, viewer.Documents.GetDocumentsByReverseUpdateOrder(0).Count());
					var tuples = viewer.Documents.GetDocumentsByReverseUpdateOrder(0).ToArray();
					Assert.Equal(2, tuples.Length);
					Assert.Equal("Oren", tuples[0].Key);
					Assert.Equal("Ayende", tuples[1].Key);

					Assert.Equal(1, viewer.Documents.GetDocumentsByReverseUpdateOrder(1).Count());
					tuples = viewer.Documents.GetDocumentsByReverseUpdateOrder(1).ToArray();
					Assert.Equal("Ayende", tuples[0].Key);
				});
			}
		}
	}
}