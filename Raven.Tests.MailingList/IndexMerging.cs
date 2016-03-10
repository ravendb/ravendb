using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class AutoIndexMerging : RavenTest
    {
        const string SampleLogfileStoreId = "123";

        [Fact]
        public async Task AutoIndexReuseFails()
        {
            var store = NewRemoteDocumentStore(fiddler: true);
            var session = store.OpenAsyncSession();

            // each of these queries will generate an auto index, that will expand on the previous auto index and include additional fields

            // Auto/Logfiles/ByUploadDateSortByUploadDate
            await FirstQuery(session);

            // Auto/Logfiles/BySavedAnalysesAndStoreIdAndUploadDateSortByUploadDate
            await SecondQuery(session);

            //  Auto/Logfiles/BySavedAnalysesAndSharedOnFacebookActionIdAndStoreIdAndUploadDateSortByUploadDate
            await ThirdQuery(session);
            var definitions = GetAutoIndexes(store);
            Assert.Equal(3, definitions.Length);

            // now lets delete the second index
            store.DatabaseCommands.DeleteIndex(definitions[1].Name);
            store.DatabaseCommands.DeleteIndex(definitions[2].Name);

            await FirstQuery(session);
            await SecondQuery(session);
            await ThirdQuery(session);

            // Auto/Logfiles/BySavedAnalysesAndSharedOnFacebookActionIdAndStoreIdAndUploadDateSortByUploadDate is able to fulfill all requests
            Assert.Equal(1, GetAutoIndexes(store).Length);
        }

        static IndexDefinition[] GetAutoIndexes(IDocumentStore store)
        {
            return store.DatabaseCommands.GetIndexes(0, 1024).Where(x => x.Name.StartsWith("Auto/")).ToArray();
        }

        static Task<int> ThirdQuery(IAsyncDocumentSession session)
        {
            return session.Query<Logfile>().Where(x => x.StoreId != SampleLogfileStoreId && x.SharedOnFacebookActionId != null).CountAsync();
        }

        static Task<int> SecondQuery(IAsyncDocumentSession session)
        {
            return session.Query<Logfile>().Where(x => x.StoreId != SampleLogfileStoreId && x.SavedAnalyses.Any()).CountAsync();
        }

        static Task<IList<string>> FirstQuery(IAsyncDocumentSession session)
        {
            var now = DateTime.UtcNow;

            RavenQueryStatistics stats;
            return session.Query<Logfile>()
                .Statistics(out stats)
                .Where(x => x.UploadDate >= now.AddMonths(-1))
                .Select(x => x.Owner)
                .Distinct()
                .Take(1024) // see 
                .ToListAsync();
        }

        class Logfile
        {
            public DateTime UploadDate { get; set; }
            public string Owner { get; set; }
            public string StoreId { get; set; }
            public string[] SavedAnalyses { get; set; }
            public string SharedOnFacebookActionId { get; set; }
        }
    }
}
