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
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var a = new FastTests.Server.Documents.Transformers.BasicTransformers())
                {
                    try
                    {
                        a.WillLoadAsFaulty().Wait();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        return;
                    }
                }
            }
        }
    }
}
