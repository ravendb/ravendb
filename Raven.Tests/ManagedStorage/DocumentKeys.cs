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
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
			}

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Read(viewer => Assert.Equal(new[] { "Ayende" }, viewer.Documents.DocumentKeys.ToArray()));
			}
		}
	}
}