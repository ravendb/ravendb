using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class LoadWithIncludeTests : RavenTest
	{
		[Fact]
		public void ChildBeforeInclude()
		{
			using(var store = NewRemoteDocumentStore())
			{
				var ids = AddTestDocs(store);
				LoadWithChildren(store, ids);
			}
		}

		[Fact]
		public void ChildViaInclude()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var ids = AddTestDocs(store);
				Array.Reverse(ids);
				LoadWithChildren(store, ids);
			}
		}

		private string[] AddTestDocs(IDocumentStore store)
		{
			var idList = new List<string>();

			using (var session = store.OpenSession())
			{
				var child = new ParentChildDoc();
				session.Store(child);
				idList.Add(child.Id);

				var parent = new ParentChildDoc();
				parent.ChildIds.Add(child.Id);
				session.Store(parent);
				idList.Add(parent.Id);

				session.SaveChanges();
			}

			return idList.ToArray();
		}

		private void LoadWithChildren(IDocumentStore store, string[] idArray)
		{
			using (var session = store.OpenSession())
			{
				var docs = session.Include<ParentChildDoc>(d => d.ChildIds).Load<ParentChildDoc>(idArray);

				foreach (var doc in docs)
				{
					Assert.NotNull(doc);

					foreach (var childId in doc.ChildIds)
					{
						var childDoc = session.Load<ParentChildDoc>(childId);
						Assert.NotNull(childDoc);
					}
				}
			}
		}
		public class ParentChildDoc
		{

			public ParentChildDoc()
			{
				ChildIds = new List<string>();
			}
			public string Id { get; set; }
			public ICollection<string> ChildIds { get; set; }
		}
	}
}