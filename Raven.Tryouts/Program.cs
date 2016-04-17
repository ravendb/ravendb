using System;
using System.Diagnostics;

#if !DNXCORE50
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

            try
            {
                for (int i = 0; i < 1000; i++)
                {
                    if(i%50==0)
                        Console.WriteLine("i = " + i);
                    using (var test = new ParallelCalculation())
                    {
                        test.ThrottlingTest();
                    }
                }
            }
            catch (Exception e)
            {

                Debugger.Break();
            }
#endif
        }
    }
}
