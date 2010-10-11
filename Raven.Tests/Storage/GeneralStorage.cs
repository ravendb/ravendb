using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class GeneralStorage : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public GeneralStorage()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
			});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanGetDocumentCounts()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(0, actions.Documents.GetDocumentsCount());

				actions.Documents.AddDocument("a", null, new JObject(), new JObject());
			});

			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(1, actions.Documents.GetDocumentsCount());

				JObject metadata;
				actions.Documents.DeleteDocument("a", null, out metadata);
			});


			db.TransactionalStorage.Batch(actions => Assert.Equal(0, actions.Documents.GetDocumentsCount()));
		}

        [Fact]
        public void CanGetDocumentAfterEmptyEtag()
        {
            db.TransactionalStorage.Batch(actions => actions.Documents.AddDocument("a", null, new JObject(), new JObject()));

            db.TransactionalStorage.Batch(actions =>
            {
                var documents = actions.Documents.GetDocumentsAfter(Guid.Empty).ToArray();
                Assert.Equal(1, documents.Length);
            });
        }

        [Fact]
        public void CanGetDocumentAfterAnEtag()
        {
            db.TransactionalStorage.Batch(actions =>
            {
                actions.Documents.AddDocument("a", null, new JObject(), new JObject());
                actions.Documents.AddDocument("b", null, new JObject(), new JObject());
                actions.Documents.AddDocument("c", null, new JObject(), new JObject());
            });

            db.TransactionalStorage.Batch(actions =>
            {
                var doc = actions.Documents.DocumentByKey("a",null);
                var documents = actions.Documents.GetDocumentsAfter(doc.Etag).Select(x => x.Key).ToArray();
                Assert.Equal(2, documents.Length);
                Assert.Equal("b", documents[0]);
                Assert.Equal("c", documents[1]);
            });
        }

        [Fact]
        public void CanGetDocumentAfterAnEtagAfterDocumentUpdateWouldReturnThatDocument()
        {
            db.TransactionalStorage.Batch(actions =>
            {
                actions.Documents.AddDocument("a", null, new JObject(), new JObject());
                actions.Documents.AddDocument("b", null, new JObject(), new JObject());
                actions.Documents.AddDocument("c", null, new JObject(), new JObject());
            });

            Guid guid = Guid.Empty;
            db.TransactionalStorage.Batch(actions =>
            {
                var doc = actions.Documents.DocumentByKey("a", null);
                guid = doc.Etag;
                actions.Documents.AddDocument("a", null, new JObject(), new JObject());
            });

            db.TransactionalStorage.Batch(actions =>
            {
                var documents = actions.Documents.GetDocumentsAfter(guid).Select(x => x.Key).ToArray();
                Assert.Equal(3, documents.Length);
                Assert.Equal("b", documents[0]);
                Assert.Equal("c", documents[1]);
                Assert.Equal("a", documents[2]);
            });
        }

		[Fact]
		public void UpdatingDocumentWillKeepSameCount()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(0, actions.Documents.GetDocumentsCount());

				actions.Documents.AddDocument("a", null, new JObject(), new JObject());

			});

			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(1, actions.Documents.GetDocumentsCount());

				actions.Documents.AddDocument("a", null, new JObject(), new JObject());
			});


			db.TransactionalStorage.Batch(actions => Assert.Equal(1, actions.Documents.GetDocumentsCount()));
		}


        [Fact]
        public void CanEnqueueAndPeek()
        {
            db.TransactionalStorage.Batch(actions => actions.Queue.EnqueueToQueue("ayende", new byte[]{1,2}));

            db.TransactionalStorage.Batch(actions => Assert.Equal(new byte[] { 1, 2 }, actions.Queue.PeekFromQueue("ayende").First().Item1));
        }

        [Fact]
        public void PoisonMessagesWillBeDeleted()
        {
            db.TransactionalStorage.Batch(actions => actions.Queue.EnqueueToQueue("ayende", new byte[] { 1, 2 }));

            db.TransactionalStorage.Batch(actions =>
            {
                for (int i = 0; i < 6; i++)
                {
                    actions.Queue.PeekFromQueue("ayende").First();
                }
                Assert.Equal(null, actions.Queue.PeekFromQueue("ayende").FirstOrDefault());
            });
        }

        [Fact]
        public void CanDeleteQueuedData()
        {
            db.TransactionalStorage.Batch(actions => actions.Queue.EnqueueToQueue("ayende", new byte[] { 1, 2 }));

            db.TransactionalStorage.Batch(actions =>
            {
                actions.Queue.DeleteFromQueue("ayende", actions.Queue.PeekFromQueue("ayende").First().Item2);
                Assert.Equal(null, actions.Queue.PeekFromQueue("ayende").FirstOrDefault());
            });
        }

		[Fact]
		public void CanGetNewIdentityValues()
		{
			db.TransactionalStorage.Batch(actions=>
			{
				var nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(1, nextIdentityValue);

				nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(2, nextIdentityValue);

			});

			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(3, nextIdentityValue);

				nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(4, nextIdentityValue);

			});
		}

		[Fact]
		public void CanGetNewIdentityValuesWhenUsingTwoDifferentItems()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(1, nextIdentityValue);

				nextIdentityValue = actions.General.GetNextIdentityValue("blogs");

				Assert.Equal(1, nextIdentityValue);

			});

			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.General.GetNextIdentityValue("blogs");

				Assert.Equal(2, nextIdentityValue);

				nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(2, nextIdentityValue);

			});
		}
	}
}
