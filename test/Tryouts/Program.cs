using System;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
	        for (int i = 0; i < 1000; i++)
	        {
		        using (var x = new FastTests.Server.Documents.Replication.ReplicationTombstoneTests())
		        {
			        x.Two_tombstones_should_replicate_in_master_master().Wait();
		        }
				Console.WriteLine(i);
			}
			//Parallel.For(0, 1000, i =>
			//{
			//    using (var x = new Fanout())
			//    {
			//        x.ShouldSkipDocumentsIfMaxIndexOutputsPerDocumentIsExceeded();
			//    }
			//    Console.WriteLine(i);
			//});
		}
    }
}

