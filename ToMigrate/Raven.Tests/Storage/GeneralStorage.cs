//-----------------------------------------------------------------------
// <copyright file="GeneralStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Embedded;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Tasks;
using Raven.Tests.Common;

using Xunit;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Storage.Esent;
using Xunit.Extensions;

namespace Raven.Tests.Storage
{
    public class GeneralStorage : RavenTest
    {
        private readonly EmbeddableDocumentStore store;
        private readonly DocumentDatabase db;

        public GeneralStorage()
        {
            store = NewDocumentStore();
            db = store.SystemDatabase;
        }

        public override void Dispose()
        {
            store.Dispose();
            base.Dispose();
        }

        [Fact]
        public void Can_query_by_id_prefix()
        {
            db.Documents.Put("abc", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);
            db.Documents.Put("Raven/Databases/Hello", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);
            db.Documents.Put("Raven/Databases/Northwind", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);
            db.Documents.Put("Raven/Databases/Sys", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);
            db.Documents.Put("Raven/Databases/Db", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);
            db.Documents.Put("Raven/Database", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);

            int nextPageStart = 0;
            var dbs = db.Documents.GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, 0, 10, CancellationToken.None, ref nextPageStart);

            Assert.Equal(4, dbs.Length);
        }

        [Fact]
        public void WhenPutAnIdWithASpace_IdWillBeAGuid()
        {
            db.Documents.Put(" ", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);

            var doc = db.Documents.GetDocumentsAsJson(0, 10, null, CancellationToken.None)
                .OfType<RavenJObject>()
                .Single();
            var id = doc["@metadata"].Value<string>("@id");
            Assert.False(string.IsNullOrWhiteSpace(id));
            Assert.DoesNotThrow(() => new Guid(id));
        }

        [Fact]
        public void CanGetDocumentCounts()
        {
            db.TransactionalStorage.Batch(actions =>
            {
                Assert.Equal(0, actions.Documents.GetDocumentsCount());

                actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
            });

            db.TransactionalStorage.Batch(actions =>
            {
                Assert.Equal(1, actions.Documents.GetDocumentsCount());

                RavenJObject metadata;
                Etag tag;
                actions.Documents.DeleteDocument("a", null, out metadata, out tag);
            });


            db.TransactionalStorage.Batch(actions => Assert.Equal(0, actions.Documents.GetDocumentsCount()));
        }

        [Fact]
        public void CanGetDocumentAfterEmptyEtag()
        {
            db.TransactionalStorage.Batch(actions => actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject()));

            db.TransactionalStorage.Batch(actions =>
            {
                var documents = actions.Documents.GetDocumentsAfter(Etag.Empty, 5, CancellationToken.None).ToArray();
                Assert.Equal(1, documents.Length);
            });
        }

        [Fact]
        public void CanGetDocumentAfterAnEtag()
        {
            db.TransactionalStorage.Batch(actions =>
            {
                actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
            });

            db.TransactionalStorage.Batch(actions =>
            {
                var doc = actions.Documents.DocumentByKey("a");
                var documents = actions.Documents.GetDocumentsAfter(doc.Etag, 5, CancellationToken.None).Select(x => x.Key).ToArray();
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
                actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
            });

            Etag etag = null;
            db.TransactionalStorage.Batch(actions =>
            {
                var doc = actions.Documents.DocumentByKey("a");
                etag = doc.Etag;
                actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
            });

            db.TransactionalStorage.Batch(actions =>
            {
                var documents = actions.Documents.GetDocumentsAfter(etag, 5, CancellationToken.None).Select(x => x.Key).ToArray();
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

                actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());

            });

            db.TransactionalStorage.Batch(actions =>
            {
                Assert.Equal(1, actions.Documents.GetDocumentsCount());

                actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
            });


            db.TransactionalStorage.Batch(actions => Assert.Equal(1, actions.Documents.GetDocumentsCount()));
        }



        [Fact]
        public void CanEnqueueAndPeek()
        {
            db.TransactionalStorage.Batch(actions => actions.Queue.EnqueueToQueue("ayende", new byte[] { 1, 2 }));

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
            db.TransactionalStorage.Batch(actions =>
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

        [Fact]
        public void check_alerts_document()
        {
            db.TransactionalStorage.Batch(actions =>
            {
                Assert.DoesNotThrow(() =>
                {
                    var doc = actions.Documents.DocumentByKey(Constants.RavenAlerts);
                    if (doc == null)
                        return;

                    throw new InvalidOperationException("Alerts document data: " + doc.DataAsJson);
                });
            });
        }
    }
}
