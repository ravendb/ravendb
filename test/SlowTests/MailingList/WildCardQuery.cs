using System.Threading.Tasks;
using FastTests;
using Raven.Client.Data;
using Xunit;

namespace SlowTests.MailingList
{
    public class WildCardQuery : RavenTestBase
    {
         [Fact]
         public async Task CanQuery()
         {
             using(var store = await GetDocumentStore())
             {
                 store.DatabaseCommands.Query("dynamic/foo", new IndexQuery
                 {
                     Query = "PortalId:0 AND Query:(*) QueryBoosted:(*)"
                 });
             }
         }
    }
}
