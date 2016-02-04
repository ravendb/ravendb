//-----------------------------------------------------------------------
// <copyright file="DocumentKeys.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Storage
{
    using System.Threading;

    public class DocumentKeys : RavenTest
    {
        [Fact]
        public void CanGetDocumentKeys()
        {
            var dataDir = NewDataPath();

            using (var tx = NewTransactionalStorage(dataDir: dataDir, runInMemory: false))
            {
                tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, RavenJObject.FromObject(new { Name = "Rahien" }), new RavenJObject()));
            }

            using (var tx = NewTransactionalStorage(dataDir: dataDir, runInMemory: false))
            {
                tx.Batch(viewer => Assert.Equal(new[] { "Ayende" }, viewer.Documents.GetDocumentsAfter(Etag.Empty,5, CancellationToken.None).Select(x=>x.Key).ToArray()));
            }
        }
    }
}
