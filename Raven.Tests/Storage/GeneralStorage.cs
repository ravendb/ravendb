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
				ShouldCreateDefaultsWhenBuildingNewDatabaseFromScratch = false
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
				Assert.Equal(0, actions.GetDocumentsCount());

				actions.AddDocument("a", null, new JObject(), new JObject());
			});

			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(1, actions.GetDocumentsCount());

				actions.DeleteDocument("a", null);
			});


			db.TransactionalStorage.Batch(actions => Assert.Equal(0, actions.GetDocumentsCount()));
		}

        [Fact]
        public void CanGetDocumentAfterEmptyEtag()
        {
            db.TransactionalStorage.Batch(actions => actions.AddDocument("a", null, new JObject(), new JObject()));

            db.TransactionalStorage.Batch(actions =>
            {
                var documents = actions.GetDocumentsAfter(Guid.Empty).ToArray();
                Assert.Equal(1, documents.Length);
            });
        }

        [Fact]
        public void CanGetDocumentAfterAnEtag()
        {
            db.TransactionalStorage.Batch(actions =>
            {
                actions.AddDocument("a", null, new JObject(), new JObject());
                actions.AddDocument("b", null, new JObject(), new JObject());
                actions.AddDocument("c", null, new JObject(), new JObject());
            });

            db.TransactionalStorage.Batch(actions =>
            {
                var doc = actions.DocumentByKey("a",null);
                var documents = actions.GetDocumentsAfter(doc.Etag).Select(x => x.Key).ToArray();
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
                actions.AddDocument("a", null, new JObject(), new JObject());
                actions.AddDocument("b", null, new JObject(), new JObject());
                actions.AddDocument("c", null, new JObject(), new JObject());
            });

            Guid guid = Guid.Empty;
            db.TransactionalStorage.Batch(actions =>
            {
                var doc = actions.DocumentByKey("a", null);
                guid = doc.Etag;
                actions.AddDocument("a", null, new JObject(), new JObject());
            });

            db.TransactionalStorage.Batch(actions =>
            {
                var documents = actions.GetDocumentsAfter(guid).Select(x => x.Key).ToArray();
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
				Assert.Equal(0, actions.GetDocumentsCount());

				actions.AddDocument("a", null, new JObject(), new JObject());

			});

			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(1, actions.GetDocumentsCount());

				actions.AddDocument("a", null, new JObject(), new JObject());
			});


			db.TransactionalStorage.Batch(actions => Assert.Equal(1, actions.GetDocumentsCount()));
		}


        [Fact]
        public void CanEnqueueAndPeek()
        {
            db.TransactionalStorage.Batch(actions => actions.EnqueueToQueue("ayende", new byte[]{1,2}));

            db.TransactionalStorage.Batch(actions => Assert.Equal(new byte[] { 1, 2 }, actions.PeekFromQueue("ayende").Item1));
        }

        [Fact]
        public void PoisonMessagesWillBeDeletes()
        {
            db.TransactionalStorage.Batch(actions => actions.EnqueueToQueue("ayende", new byte[] { 1, 2 }));

            db.TransactionalStorage.Batch(actions =>
            {
                for (int i = 0; i < 6; i++)
                {
                    actions.PeekFromQueue("ayende");
                }
                Assert.Equal(null, actions.PeekFromQueue("ayende"));
            });
        }

        [Fact]
        public void CanDeleteQueuedData()
        {
            db.TransactionalStorage.Batch(actions => actions.EnqueueToQueue("ayende", new byte[] { 1, 2 }));

            db.TransactionalStorage.Batch(actions =>
            {
                actions.DeleteFromQueue(actions.PeekFromQueue("ayende").Item2);
                Assert.Equal(null, actions.PeekFromQueue("ayende"));
            });
        }

		[Fact]
		public void CanGetNewIdentityValues()
		{
			db.TransactionalStorage.Batch(actions=>
			{
				var nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(1, nextIdentityValue);

				nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(2, nextIdentityValue);

			});

			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(3, nextIdentityValue);

				nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(4, nextIdentityValue);

			});
		}

		[Fact]
		public void CanGetNewIdentityValuesWhenUsingTwoDifferentItems()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(1, nextIdentityValue);

				nextIdentityValue = actions.GetNextIdentityValue("blogs");

				Assert.Equal(1, nextIdentityValue);

			});

			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.GetNextIdentityValue("blogs");

				Assert.Equal(2, nextIdentityValue);

				nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(2, nextIdentityValue);

			});
		}
	}
}