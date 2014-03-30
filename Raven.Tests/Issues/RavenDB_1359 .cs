// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1359 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1359 : RavenTest
	{
		[Fact]
		public void IndexThatLoadAttachmentsShouldIndexAllDocuments()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent")) // the problem occurred only on Esent
			{
				var indexWithAttachments = new RavenDB1316.Attachment_Indexing();
				indexWithAttachments.Execute(store);

				store.DatabaseCommands.PutAttachment("attachment", null,
				                                     new MemoryStream(Encoding.UTF8.GetBytes("this is a test")),
				                                     new RavenJObject());
				var tasks = new List<Task>();

				// here we have to do a big document upload because the problem occurred only when 
				// DefaultBackgroundTaskExecuter.ExecuteAllInterleaved method was performed during indexing process

				for (var i = 0; i < 5; i++)
				{
					tasks.Add(Task.Factory.StartNew(() =>
					{
						for (var j = 0; j < 1000; j++)
						{
							using (var session = store.OpenSession())
							{
								session.Store(new RavenDB1316.Item
								{
									Name = "A",
									Content = "attachment"
								});
								session.Store(new RavenDB1316.Item
								{
									Name = "B",
									Content = "attachment"
								});
								session.Store(new RavenDB1316.Item
								{
									Name = "C",
									Content = "attachment"
								});
								session.SaveChanges();
							}
						}
					}));
				}

				Task.WaitAll(tasks.ToArray());

				WaitForIndexing(store);

				IndexStats indexStats =
					store.DatabaseCommands.GetStatistics().Indexes.First(x => x.PublicName == indexWithAttachments.IndexName);
			}
		}
	}
}
