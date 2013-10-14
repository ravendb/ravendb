using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client;
using Xunit;
using Raven.Client.Extensions;
using Raven.Client.Bundles.MoreLikeThis;

namespace Raven.Tests.Bundles.MoreLikeThis
{
    public class MoreLikeThisMultiTenant : RemoteClientTest
    {
		private readonly IDocumentStore store;

        public MoreLikeThisMultiTenant()
		{
			store = NewRemoteDocumentStore();
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

        [Fact]
        public void CanQueryTenantDb()
        {
            string id;

	        const string database = "MoreLikeThisTenant";

			store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(database);

            using (var session = store.OpenSession(database))
            {

				new MoreLikeThisTests.DataIndex().Execute(store.DatabaseCommands.ForDatabase(database),store.Conventions);

                var dataQueriedFor = new MoreLikeThisTests.Data { Body = "This is a test. Isn't it great? I hope I pass my test!" };

                var list = new List<MoreLikeThisTests.Data>
				{
					dataQueriedFor,
					new MoreLikeThisTests.Data {Body = "I have a test tomorrow. I hate having a test"},
					new MoreLikeThisTests.Data {Body = "Cake is great."},
					new MoreLikeThisTests.Data {Body = "This document has the word test only once"},
					new MoreLikeThisTests.Data {Body = "test"},
					new MoreLikeThisTests.Data {Body = "test"},
					new MoreLikeThisTests.Data {Body = "test"},
					new MoreLikeThisTests.Data {Body = "test"}
				};
                list.ForEach(session.Store);
                session.SaveChanges();

                id = session.Advanced.GetDocumentId(dataQueriedFor);
                WaitForIndexing(store, database);
            }

            using (var session = store.OpenSession(database))
            {
                var list = session.Advanced.MoreLikeThis<MoreLikeThisTests.Data, MoreLikeThisTests.DataIndex>(new MoreLikeThisQuery
                                                                                                              {
                                                                                                                  DocumentId = id,
                                                                                                                  Fields = new[] {"Body"}
                                                                                                              });

                Assert.NotEmpty(list);
            }
        }
    }
}
