using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1395 : RavenTestBase
	{
		public class DataEntry
		{
			public string Id { get; set; }

			public string Data { get; set; }
		}

		public class DataEntryIndex : AbstractIndexCreationTask<DataEntry>
		{
			public DataEntryIndex()
			{
				Map = docs => from doc in docs
								select new { doc.Data, DataTransformed = "Prefix_" + doc.Data };
			}
		}

		[Fact]
		public void Using_WaitForNonStaleResultsAsOfLastWrite_with_query_streaming_should_throw()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new DataEntryIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new DataEntry {Data = "Foo"});
					session.Store(new DataEntry {Data = "Bar"});

					session.SaveChanges();
				}				

				using (var session = store.OpenSession())
				{
					var query = session.Query<DataEntry, DataEntryIndex>().Customize(x => x.WaitForNonStaleResultsAsOfLastWrite());

					Assert.Throws<NotSupportedException>(() =>
					{
						using (session.Advanced.Stream(query))
						{
						}
					});

					query = session.Query<DataEntry, DataEntryIndex>().Customize(x => x.WaitForNonStaleResults());

					Assert.Throws<NotSupportedException>(() =>
					{
						using (session.Advanced.Stream(query))
						{
						}
					});
				}
			}
		}
	}
}
