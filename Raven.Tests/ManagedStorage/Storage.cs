using System.IO;
using Newtonsoft.Json.Linq;
using Raven.Storage.Managed;
using Xunit;

namespace Raven.Storage.Tests
{
	public class Storage : TxStorageTest
	{
		[Fact]
		public void CanCreateNewFile()
		{
			using (new TransactionalStorage("test"))
			{

			}
		}

		[Fact]
		public void CanCreateNewFileAndThenOpenIt()
		{
			using (new TransactionalStorage("test"))
			{

			}

			using (new TransactionalStorage("test"))
			{
			}
		}

		[Fact]
		public void CanHandleTruncatedFile()
		{
			var fileName = Path.Combine("test", "storage.raven");
			long lengthAfterFirstTransaction;
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));

				lengthAfterFirstTransaction = new FileInfo(fileName).Length;

				tx.Write(mutator => mutator.Documents.AddDocument("Oren", null, JObject.FromObject(new { Name = "Eini" }), new JObject()));

			}

			using (var fileStream = File.Open(fileName, FileMode.Open))//simulate crash in the middle of a transaction write
			{
				fileStream.SetLength(lengthAfterFirstTransaction + (fileStream.Length - lengthAfterFirstTransaction) / 2);
			}

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Read(viewer => Assert.NotNull(viewer.Documents.DocumentByKey("Ayende", null)));
				tx.Read(viewer => Assert.Null(viewer.Documents.DocumentByKey("Oren", null)));
			}
		}

	}
}