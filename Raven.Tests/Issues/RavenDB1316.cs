// -----------------------------------------------------------------------
//  <copyright file="RavenDB1316.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB1316 : RavenTest
	{
		public class Item
		{
			public string Content { get; set; }
			public string Name { get; set; }
		}

		public class Attachment_Indexing : AbstractIndexCreationTask<Item>
		{
			public Attachment_Indexing()
			{
				Map = items =>
				      from item in items
				      select new
				      {
					      Search = new[]
					      {
							item.Name,
							LoadAttachmentForIndexing(item.Content)
					      }
				      };
				Index("Search", FieldIndexing.Analyzed);
			}
		}

		[Fact]
		public void CanIndexAttachments()
		{
			using (var store = NewDocumentStore())
			{
				new Attachment_Indexing().Execute(store);

				store.DatabaseCommands.PutAttachment("test", null,
				                                     new MemoryStream(Encoding.UTF8.GetBytes("this is a test")),
				                                     new RavenJObject());

				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "ayende",
						Content = "test"
					});
					session.SaveChanges();
				}
				WaitForIndexing(store);


				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Advanced.LuceneQuery<Item, Attachment_Indexing>().WhereEquals("Search", "ayende"));
					Assert.NotEmpty(session.Advanced.LuceneQuery<Item, Attachment_Indexing>().WhereEquals("Search", "test"));
				}

			}
		}

	}
}