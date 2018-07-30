using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Queries.Parser;
using FastTests.Sparrow;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Authentication;
using SlowTests.Bugs.MapRedue;
using SlowTests.Client;
using SlowTests.Client.Attachments;
using SlowTests.Issues;
using SlowTests.MailingList;
using Sparrow.Logging;
using StressTests.Client.Attachments;
using Xunit;

namespace Tryouts
{

    public class SubscriptionsIncludeTest : RavenTestBase
    {
        [Fact]
        public void DoStuff()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Address
                    {
                        City = "Kiryat Shmona"
                    }, "addresses/1");
                    User entity = new User
                    {
                        Name = "foobar",
                        AddressId = "addresses/1"
                    };
                    session.Store(entity);
                    session.SaveChanges();
                    session.CountersFor(entity).Increment("Modifications");
                    session.SaveChanges();
                    
                }
                var subsId = store.Subscriptions.Create<User>(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions<User>()
                {
                    Projection = x => new
                    {
                        Foo = RavenQuery.Counter(x, "Modifications"),
                        x.AddressId
                    }
                });

                var subsWorker = store.Subscriptions.GetSubscriptionWorker(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    CloseWhenNoDocsLeft = true
                });
                subsWorker.Run(x =>
                {
                    Console.WriteLine(x.Items[0].RawResult["Foo"].ToString());
                }).Wait();
            }
        }
    }
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            try
            {
                using (var test = new SubscriptionsIncludeTest())
                {
                    test.DoStuff();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            
        }
    }
}
