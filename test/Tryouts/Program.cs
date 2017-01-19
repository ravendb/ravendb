using System;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;
using Sparrow.Platform;
using System.Linq;
using FastTests.Blittable;
using FastTests.Issues;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Queries;
using FastTests.Voron.FixedSize;
using FastTests.Voron.RawData;
using SlowTests.Issues;
using SlowTests.Tests;
using SlowTests.Voron;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(".." + i);
                using (var a = new Full())
                    a.CanBackupAndRestore();
            }
            

            //using (var a = new RavenDB_5500())
            //    a.WillThrowIfIndexPathIsNotDefinedInDatabaseConfiguration();

            //using (var a = new RecoveryMultipleJournals())
            //    a.CorruptingOneTransactionWillThrow();

            //using (var a = new RecoveryMultipleJournals())
            //    a.CorruptingAllLastTransactionsConsideredAsEndOfJournal();

            //using (var a = new DocumentsCrud())
            //    a.EtagsArePersistedWithDeletes();

            //using (var a = new LargeFixedSizeTreeBugs())
            //    a.DeleteRangeShouldModifyPage();

            //using (var a = new RecoveryMultipleJournals())
            //    a.CorruptingLastTransactionsInNotLastJournalShouldThrow();

        }
    }
}

