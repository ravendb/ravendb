// -----------------------------------------------------------------------
//  <copyright file="Tobias2.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Reflection;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class SortOnNullableTests : RavenTest
	{

		private SortOnNullableEntity[] data = new[]
		{
			new SortOnNullableEntity {Text = "fail", Num = null},
			new SortOnNullableEntity {Text = "foo", Num = 2},
			new SortOnNullableEntity {Text = "boo", Num = 1}
		};

		protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
		{
		}

		[Fact]
		public void SortOnNullable()
		{
			using (var store = NewDocumentStore())
			{
				new SortOnNullableEntity_Search().Execute(store);
				using (var session = store.OpenSession())
				{
					foreach (var d in data) session.Store(d);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats = null;
					var tst = session.Advanced.LuceneQuery<SortOnNullableEntity, SortOnNullableEntity_Search>()
						.WaitForNonStaleResults()
						.Statistics(out stats)
						.OrderBy(x => x.Num)
						.ToList();

					Assert.NotEmpty(tst);
					Assert.Equal("fail", tst[0].Text);
					Assert.Equal("boo", tst[1].Text);
					Assert.Equal("foo", tst[2].Text);
					Assert.False(stats.IsStale, "Index is stale.");
				}
			}
		}

		public class SortOnNullableEntity
		{
			public string Id { get; set; }
			public string Text { get; set; }
			public int? Num { get; set; }
		}

		public class SortOnNullableEntity_Search : AbstractIndexCreationTask<SortOnNullableEntity>
		{
			public SortOnNullableEntity_Search()
			{
				Map = docs => from d in docs
							  select new
							  {
								  Text = d.Text,
								  Num = d.Num
							  };

				Index(x => x.Text, FieldIndexing.Analyzed);
				Index(x => x.Num, FieldIndexing.Default);

				Sort(x => x.Num, SortOptions.Int);
			}
		}
	}
}