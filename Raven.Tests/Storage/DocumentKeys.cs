//-----------------------------------------------------------------------
// <copyright file="DocumentKeys.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Storage
{
	public class DocumentKeys : RavenTest
	{


		[Fact]
		public void CanGetDocumentKeys()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, RavenJObject.FromObject(new { Name = "Rahien" }), new RavenJObject()));
			}

			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(viewer => Assert.Equal(new[] { "Ayende" }, viewer.Documents.GetDocumentsAfter(Guid.Empty,5).Select(x=>x.Key).ToArray()));
			}
		}
	}
}
