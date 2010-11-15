using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Tests.ManagedStorage
{
	public class Documents : TxStorageTest
	{
		[Fact]
		public void CanAddAndRead()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));

				JObject document = null;
                tx.Batch(viewer =>
				{
					document = viewer.Documents.DocumentByKey("Ayende", null).DataAsJson;
				});

				Assert.Equal("Rahien", document.Value<string>("Name"));
			}
		}

        [Fact]
        public void CanUpdateDocumentThenReadIt()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));

                tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Oren" }), new JObject()));

                tx.Batch(x => Assert.Equal(1, x.Documents.GetDocumentsCount()));

                JObject document = null;
                tx.Batch(viewer =>
                {
                    document = viewer.Documents.DocumentByKey("Ayende", null).DataAsJson;
                });

                Assert.Equal("Oren", document.Value<string>("Name"));
            }
        }


        [Fact]
        public void CanUpdateDocumentThenReadItWhenThereAreManyDocs()
        {
            using (var tx = NewTransactionalStorage())
            {
                for (int i = 0; i < 11; i++)
                {
                    tx.Batch(mutator => mutator.Documents.AddDocument("docs/"+i, null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
                    
                }

                tx.Batch(mutator => mutator.Documents.AddDocument("docs/0", null, JObject.FromObject(new { Name = "Oren" }), new JObject()));

                tx.Batch(x => Assert.Equal(11, x.Documents.GetDocumentsCount()));

                JObject document = null;
                tx.Batch(viewer =>
                {
                    document = viewer.Documents.DocumentByKey("docs/0", null).DataAsJson;
                });

                Assert.Equal("Oren", document.Value<string>("Name"));
            }
        }


		

		[Fact]
		public void CanAddAndReadFileAfterReopen()
		{
			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
			}

			using (var tx = NewTransactionalStorage())
			{
				JObject document = null;
                tx.Batch(viewer =>
				{
					document = viewer.Documents.DocumentByKey("Ayende", null).DataAsJson;
				});

				Assert.Equal("Rahien", document.Value<string>("Name"));
			}
		}

		[Fact]
		public void CanDeleteFile()
		{
			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
				JObject metadata;
                tx.Batch(mutator => mutator.Documents.DeleteDocument("Ayende", null, out metadata));
			}

			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(viewer => Assert.Null(viewer.Documents.DocumentByKey("Ayende", null)));

			}
		}

		[Fact]
		public void CanCountDocuments()
		{
			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
                tx.Batch(accessor => Assert.Equal(1, accessor.Documents.GetDocumentsCount()));
				JObject metadata;
                tx.Batch(mutator => mutator.Documents.DeleteDocument("Ayende", null, out metadata));

                tx.Batch(accessor => Assert.Equal(0, accessor.Documents.GetDocumentsCount()));

			}

			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(viewer => Assert.Null(viewer.Documents.DocumentByKey("Ayende", null)));
                tx.Batch(accessor => Assert.Equal(0, accessor.Documents.GetDocumentsCount()));
			}
		}
	}
}