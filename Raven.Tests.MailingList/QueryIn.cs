// -----------------------------------------------------------------------
//  <copyright file="QueryIn.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class QueryIn : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			var idents = new[] { 1, 2, 3, 4, 5, 6, 7 };
			var index = 0;

			using (var store = NewDocumentStore())
			{
				for (var i = 0; i < 64; i++)
				{
					using (var session = store.OpenSession())
					{
						for (var j = 0; j < 10; j++)
							session.Store(new MyEntity
							{
								ImageId = idents[index++ % idents.Length],
							});
						session.SaveChanges();
					}
				}

				store.DatabaseCommands.PutIndex("TestIndex", new IndexDefinition
				{
					Map = @"docs.MyEntities.Select(entity => new {
                                    Text = entity.Text,
                                    ImageId = entity.ImageId
                                })",
					Indexes = { { "Text", FieldIndexing.Analyzed } }
				});

				WaitForUserToContinueTheTest(store);

				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session
						                .Query<MyEntity>("TestIndex")
						                .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
						                .Where(x => x.ImageId.In(new[] {67, 66, 78, 99, 700, 6})));
					Assert.NotEmpty(session
											.Query<MyEntity>("TestIndex")
											.Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
											.Where(x => x.ImageId.In(new[] { 67, 23, 66, 78, 99, 700, 6 })));
				}
			}
		}

		public class MyEntity
		{
			public string Id { get; set; }
			public int ImageId { get; set; }
			public string Text { get; set; }
		}

	}
}