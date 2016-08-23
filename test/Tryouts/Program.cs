using System;
using FastTests.Server.Documents.Replication;

namespace Tryouts
{

    public class Program
    {
        static void Main(string[] args)
        {
	        for (int i = 0; i < 1000; i++)
	        {
				Console.WriteLine(i);
		        using (var f = new ReplicationConflictsTests())
		        {
			        f.Conflict_should_work_on_master_slave_slave().Wait();
		        }
	        }
        }
    }
}

