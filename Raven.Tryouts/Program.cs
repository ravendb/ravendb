using System;
#if !DNXCORE50

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
            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine("i = " + i);
                using (var testServerFixture = new TestServerFixture())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        Console.WriteLine("j = " + j);
                        using (var querying = new Querying())
                        {
                            querying.SetFixture(testServerFixture);
                            querying.CanStreamQueryResult();
                        }
                        using (var querying = new Querying())
                        {
                            querying.SetFixture(testServerFixture);
                            querying.CanGetFacets();
                        }
                    }
                }
            }
#endif
        }
    }
}
