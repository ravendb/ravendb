using FastTests.Server.Documents.Replication;

namespace Tryouts
{
  
    public class Program
    {
        static void Main(string[] args)
        {
            using (var x = new ReplicationBasicTests())
            {
                x.Master_master_replication_with_multiple_PUTS_should_work().Wait();
            }
        }
    }
}

