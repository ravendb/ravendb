using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Patching;
using FastTests.Server.Documents.Replication;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            //LoggingSource.Instance.SetupLogMode(LogMode.Information, "E:\\Work");

            //Parallel.For(0, 100, i =>
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var store = new SlowTests.Voron.Bugs.DataInconsistencyRepro())
                {
                    store.FaultyOverflowPagesHandling_CannotModifyReadOnlyPages(initialNumberOfDocs: 1000, numberOfModifications: 5000, seed: 662314505);
                }
            }
            //);
        }
    }

}

