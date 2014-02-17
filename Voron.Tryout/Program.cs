using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Tryout
{
    internal unsafe class Program
    {
        private static void Main()
        {
            for (int ix = 0; ix < 5; ix++)
            {
                const int size = 10 * 1000;

                var sp = Stopwatch.StartNew();
                using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.CreateTree(tx, "test");
                        tx.Commit();
                    }

                    var buffer = new byte[100];
                    var total = 0;
                    for (int i = 0; i < size; i++)
                    {
                        using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                        {
                            var t = env.CreateTree(tx, "test");

                            for (int j = 0; j < 10; j++)
                            {
                                t.Add(tx, (total++).ToString("00000000"), new MemoryStream(buffer));
                            }

                            tx.Commit();
                        }
                    }

                    for (int i = 0; i < size; i++)
                    {
                        using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                        {
                            var t = env.CreateTree(tx, "test");
                            var treeIterator = t.Iterate(tx);
                            for (int j = 0; j < 10; j++)
                            {
                                if (treeIterator.Seek(Slice.BeforeAllKeys) == false)
                                {
                                    t.Delete(tx, treeIterator.CurrentKey);
                                }
                            }

                            tx.Commit();
                        }
                    }

                   
                }

                Console.WriteLine(sp.Elapsed);
            }
        }
    }
}