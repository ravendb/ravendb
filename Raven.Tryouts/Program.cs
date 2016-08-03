using System;
using System.Diagnostics;
using Raven.Tests.Raft.Client;
#if !DNXCORE50
using Raven.Tests.Sorting;
using Raven.SlowTests.RavenThreadPool;
using Raven.Tests.Core;
using Raven.Tests.Core.Commands;
using Raven.Tests.Issues;
using Raven.Tests.MailingList;
using Raven.Tests.FileSystem.ClientApi;
#endif

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
#if !DNXCORE50

            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var x = new WithFailovers())
                {
                    x.ReadFromLeaderWriteToLeaderWithFailoversShouldWork(5);
                }
            }

#endif
        }
    }
}
