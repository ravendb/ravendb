using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Storage.Managed;
using Xunit;

namespace Raven.Storage.Tests
{
	public class DocumentKeys : TxStorageTest
	{


		[Fact]
		public void CanGetDocumentKeys()
		{
			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
			}

			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(viewer => Assert.Equal(new[] { "Ayende" }, viewer.Documents.GetDocumentsAfter(Guid.Empty).Select(x=>x.Key).ToArray()));
			}
		}
	}
}