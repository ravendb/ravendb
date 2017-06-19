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
            using (var a  = new FastTests.Client.Subscriptions.SubscriptionOperationsSignaling())
            {
                a.WaitOnSubscriptionStopDueToSubscriberError();
            }
        }
    }
}
