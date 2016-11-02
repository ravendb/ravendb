using BenchmarkDotNet.Running;

namespace Voron.Benchmark
{
    public class Program
    {
        public static void Main()
        {
            //BenchmarkRunner.Run<BTree.BTreeFillRandom>();
            //BenchmarkRunner.Run<BTree.BTreeFillSequential>();
            //BenchmarkRunner.Run<BTree.BTreeReadAndIterate>();
            BenchmarkRunner.Run<BTree.BTreeInsertRandom>();
            //BenchmarkRunner.Run<Table.TableFillSequential>();
            //BenchmarkRunner.Run<Table.TableFillRandom>();
            //BenchmarkRunner.Run<Table.TableReadAndIterate>();
            //BenchmarkRunner.Run<Table.TableInsertRandom>();

            //var bench = new BTree.BTreeInsertRandom();
            //bench.Setup();
            //bench.InsertRandomOneTransaction();
        }
    }
}
