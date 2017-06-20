using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using System;
using System.Linq.Expressions;
using static FastTests.Server.Replication.ReplicationBasicTests;
using Raven.Tests.Core.Utils.Entities;
using Lambda2Js;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Expression<Func<Versioned<User>, bool>> a = b => b.Previous.Name != b.Current.Name;
            Console.WriteLine(a.CompileToJavascript());

            //using (var a  = new FastTests.Client.Subscriptions.SubscriptionOperationsSignaling())
            //{
            //    a.WaitOnSubscriptionStopDueToSubscriberError();
            //}
        }
    }
}
