using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;

namespace Tryouts
{
  
    public class Program
    {
        static void Main(string[] args)
        {
            using (var f = new FastTests.Server.Documents.Replication.ReplicationBasicTests())
            {
                f.Master_slave_replication_with_multiple_PUTS_should_work().Wait();
            }

        }
    }
}

