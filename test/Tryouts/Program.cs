using System;
using FastTests.Server.Documents.Replication;

namespace Tryout
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine( i);
				using (var test = new ReplicationBasicTests())
					test.Master_master_replication_from_etag_zero_without_conflict_should_work().Wait();
				using (var test = new ReplicationBasicTests())
					test.Master_master_replication_with_multiple_PUTS_should_work().Wait();
				using (var test = new ReplicationBasicTests())
					test.Master_slave_replication_from_etag_zero_should_work().Wait();
				using (var test = new ReplicationBasicTests())
					test.Master_slave_replication_with_multiple_PUTS_should_work().Wait();
			}
		}
    }
}