using System;
using System.IO;
using SlowTests.Voron;
using Voron;

namespace Tryouts
{
    public class Program
    {
        public const long GB = 1024L * 1024 * 1024;
        public const long MB = 1024L * 1024;

        const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
        public static Random Rand = new Random(123);
        private static long BUFF_SIZE = 10 * MB;

        public static void Main(string[] args)
        {
            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine("Run #" + i);
                var test = new HugeTransactions();
                test.LZ4TestAbove2GB(3L * 1024 * 1024);
            }

            for (int i = 0; i < 5; i++)
            {
                Console.WriteLine("Run BT 2 #" + i);
                var test = new HugeTransactions();
                test.CanWriteBigTransactions(2);
            }

            for (int i = 0; i < 3; i++)
            {
                Console.WriteLine("Run BT 6 #" + i);
                var test = new HugeTransactions();
                test.CanWriteBigTransactions(6);
            }


            Console.WriteLine("Press any key for next test");
            Console.ReadKey();


            if (args.Length == 1)
                BUFF_SIZE = Convert.ToInt32(args[0]) * MB;

            BUFF_SIZE = 900*MB;

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(@"C:\zzzzTest")))
            {
                var value = new byte[BUFF_SIZE];
                Console.WriteLine("Filling " + PrintSize(BUFF_SIZE) + " buffer with random values");
                new Random().NextBytes(value);

                Console.WriteLine("Add buffer");
                using (var tx = env.WriteTransaction())
                {
                    // env.Options.DataPager.EnsureContinuous(0, 256 * 1024);
                    var tree = tx.CreateTree("test1");

                    for (int i = 0; i < 8; i++)
                    {
                        var ms1 = new MemoryStream(value);
                        ms1.Position = 0;
                        tree.Add("treeKeyAA" + i, ms1);
                    }


                    tx.Commit();
                }

                Console.WriteLine("Add buffer 2");
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("test2");
                    var ms1 = new MemoryStream(value);
                    ms1.Position = 0;
                    tree.Add("treeKey12", ms1);

                    var ms2 = new MemoryStream(value);
                    ms2.Position = 0;
                    tree.Add("treeKey13", ms2);

                    tx.Commit();
                }
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Done !");
            Console.ResetColor();
        }

        private static string PrintSize(long size)
        {
            float sum;
            string postfix;
            if (size >= GB)
            {
                sum = (size / GB);
                postfix = "GB";
            }
            else if (size >= MB)
            {
                sum = (size / MB);
                postfix = "MB";
            }
            else
            {
                sum = size;
                postfix = "B";
            }

            return $"{sum:#.##}{postfix} ({size:#,#})";
        }


    }
}
