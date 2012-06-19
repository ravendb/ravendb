//-----------------------------------------------------------------------
// <copyright file="DateRanges.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class DateRanges : LocalClientTest
	{
		[Fact]
		public void CanQueryByDate()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{

					session.Store(new Record
					{
						Date = new DateTime(2001,1,1)
					});
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("Date",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Date}"
												});


				using(var session = store.OpenSession())
				{
					var result = session.Advanced.LuceneQuery<Record>("Date")
						.WhereEquals("Date", new DateTime(2001, 1, 1))
						.WaitForNonStaleResults()
						.ToList();

					Assert.Equal(1, result.Count);
				}
			}
		}


		[Fact]
		public void CanQueryByDateRange_LowerThan()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Record
					{
						Date = new DateTime(2001, 1, 1)
					});
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("Date",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Date}"
												});


				using (var session = store.OpenSession())
				{
					var result = session.Advanced.LuceneQuery<Record>("Date")
						.Where("Date:[* TO " + DateTools.DateToString(new DateTime(2001, 1, 2), DateTools.Resolution.MILLISECOND) +"]")
						.WaitForNonStaleResults()
						.ToList();

					Assert.Equal(1, result.Count);
				}
			}
		}


		[Fact]
		public void CanQueryByDateRange_GreaterThan()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Record
					{
						Date = new DateTime(2001, 1, 1)
					});
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("Date",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Date}"
												});


				using (var session = store.OpenSession())
				{
					var result = session.Advanced.LuceneQuery<Record>("Date")
						.Where("Date:[" + DateTools.DateToString(new DateTime(2000, 1, 1), DateTools.Resolution.MILLISECOND) + " TO NULL]")
						.WaitForNonStaleResults()
						.ToList();

					Assert.Equal(1, result.Count);
				}
			}
		}
		public class Record
		{
			public string Id { get; set; }
			public DateTime Date { get; set; }
		}
	}
}
