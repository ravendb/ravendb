using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using FastTests.Server.Documents.Alerts;
using FastTests.Server.Documents.Patching;
using FastTests.Server.Documents.Replication;
using FastTests.Server.Documents.SqlReplication;
using Sparrow.Json;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            byte[] bytes = Encoding.UTF8.GetBytes("{\"Results\":[");
            Console.WriteLine(bytes.ToString());
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var a = new SlowTests.Issues.RDBQA_11())
                {
                    a.SmugglerWithExcludeExpiredDocumentsShouldWork2();
                }

                using (var a = new SlowTests.Issues.RDBQA_11())
                {
                    a.SmugglerWithExcludeExpiredDocumentsShouldWork1();
                }

                using (var a = new SlowTests.Issues.RDBQA_11())
                {
                    a.SmugglerWithoutExcludeExpiredDocumentsShouldWork();
                }
            }
        }
    }

}

