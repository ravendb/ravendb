//-----------------------------------------------------------------------
// <copyright file="DocumentKeys.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Tests.ManagedStorage
{
	public class DocumentKeys : TxStorageTest
	{


		[Fact]
		public void CanGetDocumentKeys()
		{
			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
			}

			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(viewer => Assert.Equal(new[] { "Ayende" }, viewer.Documents.GetDocumentsAfter(Guid.Empty).Select(x=>x.Key).ToArray()));
			}
		}
	}
}