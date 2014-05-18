// -----------------------------------------------------------------------
//  <copyright file="DoesPreserveDocumentIdCaseWhenPatchingFullCollectionTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class DoesPreserveDocumentIdCaseWhenPatchingFullCollectionTest : RavenTest
	{
		public class FooBar
		{
			public string Name { get; set; }
		}

		[Fact]
		public void DoesPreserveDocumentIdCaseWhenPatchingFullCollection()
		{
			using (var store = NewDocumentStore(runInMemory:false))
			{
				string documentId = null;
				using (var session = store.OpenSession())
				{
					var d = new FooBar() { };
					session.Store(d);
					session.SaveChanges();

					documentId = session.Advanced.GetDocumentId(d);
				}

				using (var session = store.OpenSession())
				{
					var d = session.Load<FooBar>(documentId);

					//Demonstrates that RavenDb stores a case-sensitive document id somewhere
					Assert.Equal(documentId, session.Advanced.GetDocumentId(d));
				}

				string script = @"  var id = __document_id; this.Name = id;";

				WaitForIndexing(store);

				string typeTag = store.Conventions.FindTypeTagName(typeof(FooBar));
				store
					.DatabaseCommands
					.UpdateByIndex(
						"Raven/DocumentsByEntityName",
						new IndexQuery() { Query = "Tag:" + typeTag },
						new ScriptedPatchRequest() { Script = script },
						false)
					.WaitForCompletion();

				using (var session = store.OpenSession())
				{
					var d = session.Load<FooBar>(documentId);
					Assert.Equal("FooBars/1", d.Name);
				}
			}
		}
	}
}