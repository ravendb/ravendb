using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var store = new DocumentStore())
            {
                //using (var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions()))
                //{
                //    await subscription.Run(async batch =>
                //    {
                //        using (var session = batch.OpenAsyncSession())
                //        {
                //            foreach (var item in batch.Items)
                //            {
                //                // process message
                //                session.Delete(item.Id);
                //            }
                //            await session.SaveChangesAsync();
                //        }
                //    });
                //}
            }
        }
    }
}
