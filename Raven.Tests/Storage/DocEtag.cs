//-----------------------------------------------------------------------
// <copyright file="DocEtag.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Storage
{
	public class DocEtag : RavenTest
	{

		[Fact]
		public void EtagsAreAlwaysIncreasing()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator =>
				{
					mutator.Documents.AddDocument("Ayende", null, RavenJObject.FromObject(new { Name = "Rahien" }), new RavenJObject());
					mutator.Documents.AddDocument("Oren", null, RavenJObject.FromObject(new { Name = "Eini" }), new RavenJObject());
				});
			}

			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(viewer =>
				{
					var doc1 = viewer.Documents.DocumentByKey("Ayende", null);
					var doc2 = viewer.Documents.DocumentByKey("Oren", null);
					Assert.Equal(1, doc2.Etag.Value.CompareTo(doc1.Etag));

				});
			}

		}

		[Fact]
		public void CanGetDocumentByEtag()
		{

			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator =>
				{
					mutator.Documents.AddDocument("Ayende", null, RavenJObject.FromObject(new { Name = "Rahien" }), new RavenJObject());
					mutator.Documents.AddDocument("Oren", null, RavenJObject.FromObject(new { Name = "Eini" }), new RavenJObject());
				});
			}

			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(viewer =>
				{
					Assert.Equal(2, viewer.Documents.GetDocumentsAfter(Guid.Empty,5).Count());
					var doc1 = viewer.Documents.DocumentByKey("Ayende", null);
					Assert.Equal(1, viewer.Documents.GetDocumentsAfter(doc1.Etag.Value,5).Count());
					var doc2 = viewer.Documents.DocumentByKey("Oren", null);
					Assert.Equal(0, viewer.Documents.GetDocumentsAfter(doc2.Etag.Value,5).Count());
				});
			}
		}

		[Fact]
		public void CanGetDocumentByUpdateOrder()
		{

			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator =>
				{
					mutator.Documents.AddDocument("Ayende", null, RavenJObject.FromObject(new { Name = "Rahien" }), new RavenJObject());
					mutator.Documents.AddDocument("Oren", null, RavenJObject.FromObject(new { Name = "Eini" }), new RavenJObject());
				});
			}

			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(viewer =>
				{
					Assert.Equal(2, viewer.Documents.GetDocumentsByReverseUpdateOrder(0, 5).Count());
					var tuples = viewer.Documents.GetDocumentsByReverseUpdateOrder(0, 5).ToArray();
					Assert.Equal(2, tuples.Length);
					Assert.Equal("Oren", tuples[0].Key);
					Assert.Equal("Ayende", tuples[1].Key);

					Assert.Equal(1, viewer.Documents.GetDocumentsByReverseUpdateOrder(1, 5).Count());
					tuples = viewer.Documents.GetDocumentsByReverseUpdateOrder(1, 5).ToArray();
					Assert.Equal("Ayende", tuples[0].Key);
				});
			}
		}
	}
}
