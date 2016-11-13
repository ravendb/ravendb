using System;
using System.Diagnostics;
using FastTests.Issues;
using FastTests.Sparrow;
using FastTests.Voron.Bugs;
using Voron;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath("D:\\bench");
            storageEnvironmentOptions.TransactionsMode=TransactionsMode.Danger;
            using (var env = new StorageEnvironment(storageEnvironmentOptions))
            {
                var sp = Stopwatch.StartNew();
                long id = 0;
                for (int i = 0; i < 10000; i++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        long val = 5;
                        Slice str;
                        using (Slice.From(tx.Allocator, "vals", out str))
                        {
                            Slice valSlice;
                            using (Slice.External(tx.Allocator, (byte*)&val, sizeof(long),out valSlice))
                            {
                                var fixedSizeTree = tx.FixedTreeFor(str, valSize: 8);

                                for (int j = 0; j < 10000; j++)
                                {
                                    val += 5;
                                    fixedSizeTree.Add(id, valSlice);
                                }
                            }
                        }
                        tx.Commit();
                    }
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

