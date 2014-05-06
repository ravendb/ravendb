using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class WallaceTurner : RavenTest
	{
		public class DataResult
		{
			public DataResult()
			{
			}

			public long Id { get; set; }
			public string Address { get; set; }
			public string Price { get; set; }
			public string Url { get; set; }
			public string SiteId { get; set; }
			public string Source { get; set; }
			public DateTime CreatedOn { get; set; }
			public DateTime LastUpdated { get; set; }
			public DateTime LastModified { get; set; }
			public string State { get; set; }
			public string Postcode { get; set; }
			public string LowerIndicativePrice { get; set; }
			public string UpperIndicativePrice { get; set; }
			public string Suburb { get; set; }

			public override string ToString()
			{
				return string.Format("Address: {0}, Id: {1}, SiteId: {2}", Address, Id, SiteId);
			}

			public DataResult Clone()
			{
				return (DataResult)MemberwiseClone();
			}
		}

		public class DataResult_ByAddress : AbstractIndexCreationTask<DataResult>
		{
			public DataResult_ByAddress()
			{
				Map = docs => from doc in docs
				              select new
				              {
				              	LastModified = doc.LastModified, 
								Address = doc.Address, 
								Suburb = doc.Suburb, 
								State = doc.State
				              };
				Index(x => x.Address, FieldIndexing.Analyzed);
			}
		}

		[Fact]
		public void ShouldBeAbleToQueryUsingNull()
		{
			using(var store = NewDocumentStore())
			{
				new DataResult_ByAddress().Execute(store);
				using(var session = store.OpenSession())
				{
					session.Store(new DataResult
					{
						SiteId = "t108137341"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var dataResult = session.Query<DataResult>().First(r => r.SiteId == "t108137341");
					Assert.Null(dataResult.State);
					var results = session.Query<DataResult>().Where(r => r.State == null)
						.Customize(o => o.WaitForNonStaleResultsAsOfLastWrite()).ToList();

					Assert.NotEmpty(results);
				}
			}
		}
	}
}