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
                using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.CreateTree(tx, "test");
                        tx.Commit();
                    }

                    var sp = Stopwatch.StartNew();
                    var producer = Task.Factory.StartNew(() =>
                    {
                        int counter = 0;
                        var buffer = new Byte[98];
                        for (int i = 0; i < 10 * 1000; i++)
                        {
                            var wb = new WriteBatch();
                            for (int j = 0; j < 10; j++)
                            {
                                wb.Add((counter++).ToString("0000000000"), new MemoryStream(buffer), "test");
                            }
                            env.Writer.Write(wb);
                        }
                    });

                    var consumer = Task.Factory.StartNew(() =>
                    {
                        var total = 0;
                        while (total < 10 * 10 * 1000)
                        {
                            var wb = new WriteBatch();
                            using (var snapshot = env.CreateSnapshot())
                            {
                                using (var it = snapshot.Iterate("test"))
                                {
                                    if (it.Seek(Slice.BeforeAllKeys) == false)
                                        continue;
                                    int i = 0;
                                    do
                                    {
                                        i++;
                                        wb.Delete(it.CurrentKey, "test");
                                        total++;
                                    } while (it.MoveNext() && i < 128);
                                }
                            }
                        }
                    });

                    Task.WaitAll(consumer, producer);

                    Console.WriteLine(sp.Elapsed);
                }
            }
        }
    }
}