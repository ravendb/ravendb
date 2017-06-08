using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;
using System.Threading.Tasks;
using FastTests.Server.Documents.Notifications;
using Raven.Server.Utils;
using Raven.Client.Documents;
using RachisTests;
using Raven.Client.Util;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string Name;
        }

        public static void Main(string[] args)
        {
           for (var i=0; i<1000; i++)
            {
                try
                {
                    Console.WriteLine($"-------------------------------------New test iteration #{i} for 3 size cluster");
                    using (var test = new SubscriptionsFailover())
                    {
                        AsyncHelpers.RunSync(() => test.DistributedVersionedSubscription(3));
                    }
                    Console.WriteLine($"-------------------------------------New test iteration #{i} for 5 size cluster");
                    using (var test = new SubscriptionsFailover())
                    {
                        AsyncHelpers.RunSync(() => test.DistributedVersionedSubscription(5));
                    }
                }
                catch (Exception e)
                {

                    Console.WriteLine($"Errrrrorrrrrr::::::::::::::{e}");
                }
            }
        }
    }
}
