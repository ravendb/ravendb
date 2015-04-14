// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3207.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Embedded;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3207 : RavenTest
	{
		public class Document
		{
			public string Value { get; set; }
		}

		[Fact]
		public void Test_paralel_operations_with_multiple_EmbeddableDocumentStores()
		{
			Action storeAndRead = () =>
			{
				using (var store = new EmbeddableDocumentStore
				{
					RunInMemory = true
				})
				{
					store.Initialize();
					using (var session = store.OpenSession())
					{
						session.Store(new Document { Value = "foo" }, "documents/1");
						session.SaveChanges();
					}
					using (var session = store.OpenSession())
					{
						var doc = session.Load<Document>("documents/1");
						Assert.Equal("foo", doc.Value);
					}
				}
			};

			storeAndRead();

			var tasks = Enumerable.Range(1, 10).Select(_ => Task.Run(storeAndRead)).ToArray();

			Task.WaitAll(tasks);
		}
	}
}