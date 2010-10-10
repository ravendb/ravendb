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
			using (NewTransactionalStorage())
			{

			}
		}

		[Fact]
		public void CanCreateNewFileAndThenOpenIt()
		{
			using (NewTransactionalStorage())
			{

			}

			using (NewTransactionalStorage())
			{
			}
		}

		[Fact]
		public void CanHandleTruncatedFile()
		{
			var fileName = Path.Combine("test", "raven.log");
			long lengthAfterFirstTransaction;
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));

				lengthAfterFirstTransaction = new FileInfo(fileName).Length;

                tx.Batch(mutator => mutator.Documents.AddDocument("Oren", null, JObject.FromObject(new { Name = "Eini" }), new JObject()));

			}

			using (var fileStream = File.Open(fileName, FileMode.Open))//simulate crash in the middle of a transaction write
			{
				fileStream.SetLength(lengthAfterFirstTransaction + (fileStream.Length - lengthAfterFirstTransaction) / 2);
			}

			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(viewer => Assert.NotNull(viewer.Documents.DocumentByKey("Ayende", null)));
                tx.Batch(viewer => Assert.Null(viewer.Documents.DocumentByKey("Oren", null)));
			}
		}

	}
}