using System;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Bugs.CompiledIndexes;
using Raven.Tests.Document;
using Raven.Tests.Indexes;
using Raven.Tests.Storage;
using Raven.Tests.Views;
using Raven.Tryouts.Json;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
            //Console.WriteLine("Starting...");

            //var executeManyFileReads2 = PerfTest.ExecuteManyFileReads2(PerfTest.BigJsonPath);

            //Console.WriteLine("Ready...");

            //Console.ReadLine();
            //int i = 0;
            //while (true)
            //{
            //    PerfTest.CloneALot2(executeManyFileReads2);
            //    Console.WriteLine(i++);
            //}
		    for (int i = 0; i < 100; i++)
		    {
		    	Console.WriteLine(i);
				new BackupRestore().AfterBackupRestoreCanQueryIndex_CreatedAfterRestore();

		    }
		}
	}
}
