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
            //TODO: finish checking this test, it seems to have race condition or sometihing
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                //using (var x = new RavenDB_5390())
                //{
                //    x.Frequent_updates_of_document_should_not_cause_deadlock_in_prefetcher();
                //}
                using (var x = new RavenDB_5390())
                {
                    x.Frequent_updates_of_document_should_not_cause_deadlock_in_prefetcher();
                }                
            }

#endif
        }
    }
}
