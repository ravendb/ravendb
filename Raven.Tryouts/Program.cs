using System;
using System.Diagnostics;

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

            using (var x = new AlphaNumericSorting())
            {
                x.dynamic_query_should_work();
            }

            Console.ReadLine();
#endif
        }
    }
}
