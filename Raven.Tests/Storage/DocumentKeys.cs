//-----------------------------------------------------------------------
// <copyright file="DocumentKeys.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Storage
{
	public class DocumentKeys : RavenTest
	{
		[Fact]
		public void CanGetDocumentKeys()
		{
			var dataDir = NewDataPath();

			using (var tx = NewTransactionalStorage(dataDir: dataDir))
			{
				tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, RavenJObject.FromObject(new { Name = "Rahien" }), new RavenJObject()));
			}

			using (var tx = NewTransactionalStorage(dataDir: dataDir))
			{
				tx.Batch(viewer => Assert.Equal(new[] { "Ayende" }, viewer.Documents.GetDocumentsAfter(Etag.Empty,5).Select(x=>x.Key).ToArray()));
			}
		}
	}
}