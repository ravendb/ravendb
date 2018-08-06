using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Voron.Benchmark.BTree;
using Constants = Voron.Global.Constants;

namespace Voron.Benchmark
{
    public class Program
    {
        public static void Main()
        {
            //BenchmarkRunner.Run<BTree.BTreeFillRandom>();
            //BenchmarkRunner.Run<BTree.BTreeFillSequential>();
            //BenchmarkRunner.Run<BTree.BTreeReadAndIterate>();
            //BenchmarkRunner.Run<BTree.BTreeInsertRandom>();
            //BenchmarkRunner.Run<Table.TableFillSequential>();
            //BenchmarkRunner.Run<Table.TableFillRandom>();
            //BenchmarkRunner.Run<Table.TableReadAndIterate>();
            //BenchmarkRunner.Run<Table.TableInsertRandom>();

            Console.WriteLine("Size " + Constants.Storage.PageSize);
            var list = new List<long>();
            for (int i = 0; i < 3; i++)
            {
                var a = new BTreeFillRandom();
                {
                    a.Setup();
                    var sp = Stopwatch.StartNew();
                    a.FillRandomMultipleTransactions();
                    list.Add(sp.ElapsedMilliseconds);
                    Console.WriteLine(". " + sp.ElapsedMilliseconds);
                    a.Cleanup();
                }
            }
            Console.WriteLine($"Min {list.Min()} Max {list.Max()} Avg {list.Average()}");
        }
    }
}
