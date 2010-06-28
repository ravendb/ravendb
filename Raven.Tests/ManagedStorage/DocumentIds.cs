using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Storage.Managed;
using Xunit;

namespace Raven.Storage.Tests
{
	public class DocumentIds : TxStorageTest
	{

		[Fact]
		public void CanGetDocumentIds()
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
					var firstAndLastDocumentIds = viewer.Documents.FirstAndLastDocumentIds();
					Assert.Equal(1, firstAndLastDocumentIds.Item1);
					Assert.Equal(2, firstAndLastDocumentIds.Item2);
				});
			}
		}

		[Fact]
		public void CanGetDocumentByDocIds()
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
					var tuples = viewer.Documents.DocumentsById(1, 2).ToArray();
					Assert.Equal(2, tuples.Length);
					Assert.Equal("Ayende", tuples[0].Item1.Key);
					Assert.Equal("Oren", tuples[1].Item1.Key);
				});
			}
		}
	}
}