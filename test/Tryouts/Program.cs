using System;
using System.Diagnostics;
using AsyncFriendlyStackTrace;
using Raven.Client.Document;
using Raven.Client.Smuggler;
using Raven.SlowTests.Issues;

namespace Tryouts
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            using (var t = new FastTests.Server.OAuth.CanAuthenticate())
            {
                try
                {
                    t.CanStoreAndDeleteApiKeys().Wait();
                }
                catch (Exception e)
                {
                    //Console.WriteLine(e.Message);
                    Console.WriteLine(e.ToAsyncString());
                }
            }
        }
    }
}