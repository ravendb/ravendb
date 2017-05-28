using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Patching;
using FastTests.Server.NotificationCenter;
using Orders;
using Org.BouncyCastle.Crypto.Tls;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server.Config;
using SlowTests.Smuggler;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 30; i++)
            {
                Console.WriteLine(i);
                Parallel.For(0, 5, _ =>
                {
                    using (var a = new AdvancedPatching())
                    {
                       a.CanUseMathFloor().Wait();
                    }
                });
            }
        }
    }
}
