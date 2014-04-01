// -----------------------------------------------------------------------
//  <copyright file="Embedded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Transactions;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.DTC
{
	public class Embedded : RavenTest
	{
		[Fact]
		public void AllowNonAuthoritativeInformationAlwaysWorks()
		{
			const int callCount = 10000;

            using (var documentStore = NewDocumentStore(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(documentStore);

				int documentId;

				// create test document
				using (IDocumentSession session = documentStore.OpenSession())
				{
					var testDocument = new TestDocument();
					session.Store(testDocument);
					session.SaveChanges();

					documentId = testDocument.Id;
				}

				// do many 'set property and check value' tests
				for (int i = 0; i < callCount; i++)
				{
					using (IDocumentSession session = documentStore.OpenSession())
					using (var tx = new TransactionScope())
					{
						session.Load<TestDocument>(documentId).Value = i;
						session.SaveChanges();

						tx.Complete();
					}

					using (IDocumentSession session = documentStore.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						var loadedDoc = session.Load<TestDocument>(documentId);
						Assert.Equal(i, loadedDoc.Value);
					}
				}
			}
		}


	    #region Nested type: TestDocument

		private class TestDocument
		{
			public int Id { get; set; }
			public int Value { get; set; }
		}

		#endregion 
	}
}