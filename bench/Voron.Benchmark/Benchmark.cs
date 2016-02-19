using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Voron.Benchmark
{
    public static class Benchmark
    {
        public static void Time(string name, Action<Stopwatch> action, bool delete = true)
        {
            if (delete)
                DeleteDirectory(Configuration.Path);

            var sp = new Stopwatch();
            Console.Write("{0,-35}: running...", name);
            action(sp);

            Console.WriteLine("\r{0,-35}: {1,10:#,#} ms {2,10:#,#} ops / sec", name, sp.ElapsedMilliseconds, Configuration.Transactions * Configuration.ItemsPerTransaction / sp.Elapsed.TotalSeconds);
        }

        private static void DeleteDirectory(string dir)
        {
            if (Directory.Exists(dir) == false)
                return;

            for (int i = 0; i < 10; i++)
            {
                try
                {
                    Directory.Delete(dir, true);
                    return;
                }
                catch (DirectoryNotFoundException)
                {
                    return;
                }
                catch (Exception)
                {
                    Thread.Sleep(13);
                }
            }

            Directory.Delete(dir, true);
        }
    }
}
