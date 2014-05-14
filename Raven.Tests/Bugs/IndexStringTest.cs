using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class IndexStringTest : RavenTest
	{
		[Fact]
		public void IndexStringAsStringEvenWhenItLooksLikeDateTime()
		{
			using (var store = NewDocumentStore())
			{
				new DataItemIndex().Execute(store);

				const string stringThatLooksLikeADateTime = "2014-05-13T12:04:13Z";
				const string stringThatDoesNotLookLikeADateTime = "NotDate-2014-05-13T12:04:13Z";

				using (var session = store.OpenSession())
				{
					session.Store(new DataItem {Code = stringThatLooksLikeADateTime});
					session.Store(new DataItem {Code = stringThatDoesNotLookLikeADateTime});
					session.SaveChanges();
				}

				WaitForIndexing(store);

				// searching for "regular" strings works fine.
				var result2 = store.DatabaseCommands.Query("DataItemIndex", new IndexQuery {Query = "__document_id: dataitems/2"}, null, indexEntriesOnly: true);
				Assert.Equal(stringThatDoesNotLookLikeADateTime, result2.Results.Single()["Code"]);

				// but searching for strings that happen to be in the format of a DateTime fails.
				var result1 = store.DatabaseCommands.Query("DataItemIndex", new IndexQuery {Query = "__document_id: dataitems/1"}, null, indexEntriesOnly: true);
				Assert.Equal(stringThatLooksLikeADateTime, result1.Results.Single()["Code"]);
			}
		}

		public class DataItem
		{
			public string Code { get; set; }
		}

		public class DataItemIndex : AbstractIndexCreationTask<DataItem>
		{
			public DataItemIndex()
			{
				Map = items => from item in items
							   select new { Code = (object)item.Code is DateTime ? ((DateTime)(object)item.Code).ToString("g") : item.Code };

				Sort(i => i.Code, SortOptions.String);
			}
		}
	}
}