// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4210.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_4210 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void can_get_etag_after_skip(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
                });

                storage.Batch(actions =>
                {
                    var etag = actions.Documents.GetEtagAfterSkip(Etag.Empty, 3, CancellationToken.None);
                    var docC = actions.Documents.DocumentByKey("c");
                    Assert.Equal(docC.Etag, etag);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_get_last_etag_after_skip_even_when_requesting_more(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
                });

                storage.Batch(actions =>
                {
                    var etag = actions.Documents.GetEtagAfterSkip(Etag.Empty, 5, CancellationToken.None);
                    var docC = actions.Documents.DocumentByKey("c");
                    Assert.Equal(docC.Etag, etag);

                    etag = actions.Documents.GetEtagAfterSkip(etag, 5, CancellationToken.None);
                    Assert.Equal(docC.Etag, etag);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_get_etag_after_skip_one_by_one1(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
                });

                storage.Batch(actions =>
                {
                    var etag = actions.Documents.GetEtagAfterSkip(Etag.Empty, 1, CancellationToken.None);
                    var docB = actions.Documents.DocumentByKey("b");
                    Assert.Equal(docB.Etag, etag);

                    etag = actions.Documents.GetEtagAfterSkip(docB.Etag, 1, CancellationToken.None);
                    var docC = actions.Documents.DocumentByKey("c");
                    Assert.Equal(docC.Etag, etag);

                    etag = actions.Documents.GetEtagAfterSkip(docC.Etag, 1, CancellationToken.None);
                    Assert.Equal(docC.Etag, etag);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_get_etag_after_skip_one_by_one2(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("d", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("e", null, new RavenJObject(), new RavenJObject());
                });

                storage.Batch(actions =>
                {
                    var docA = actions.Documents.DocumentByKey("a");
                    var etag = actions.Documents.GetEtagAfterSkip(docA.Etag, 1, CancellationToken.None);
                    var docB = actions.Documents.DocumentByKey("b");
                    Assert.Equal(docB.Etag, etag);

                    etag = actions.Documents.GetEtagAfterSkip(docB.Etag, 1, CancellationToken.None);
                    var docC = actions.Documents.DocumentByKey("c");
                    Assert.Equal(docC.Etag, etag);

                    etag = actions.Documents.GetEtagAfterSkip(docC.Etag, 1, CancellationToken.None);
                    var docD = actions.Documents.DocumentByKey("d");
                    Assert.Equal(docD.Etag, etag);

                    etag = actions.Documents.GetEtagAfterSkip(docD.Etag, 1, CancellationToken.None);
                    var docE = actions.Documents.DocumentByKey("e");
                    Assert.Equal(docE.Etag, etag);

                    etag = actions.Documents.GetEtagAfterSkip(docE.Etag, 1, CancellationToken.None);
                    Assert.Equal(docE.Etag, etag);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_get_etag_after_skip_after_delete1(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());

                    RavenJObject _;
                    Etag __;
                    actions.Documents.DeleteDocument("b", null, out _, out __);
                });

                storage.Batch(actions =>
                {
                    var etag = actions.Documents.GetEtagAfterSkip(Etag.Empty, 1, CancellationToken.None);
                    var docC = actions.Documents.DocumentByKey("c");
                    Assert.Equal(docC.Etag, etag);

                    etag = actions.Documents.GetEtagAfterSkip(docC.Etag, 1, CancellationToken.None);
                    Assert.Equal(docC.Etag, etag);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_get_etag_after_skip_after_delete2(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                Etag etagB = null;
                storage.Batch(actions =>
                {
                    actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());

                    RavenJObject _;
                    actions.Documents.DeleteDocument("b", null, out _, out etagB);
                });

                storage.Batch(actions =>
                {
                    var etag = actions.Documents.GetEtagAfterSkip(etagB, 1, CancellationToken.None);
                    var docC = actions.Documents.DocumentByKey("c");
                    Assert.Equal(docC.Etag, etag);

                    etag = actions.Documents.GetEtagAfterSkip(docC.Etag, 1, CancellationToken.None);
                    Assert.Equal(docC.Etag, etag);
                });
            }
        }
    }
}
