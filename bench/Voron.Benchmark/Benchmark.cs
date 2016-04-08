using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Voron.Benchmark
{
    public interface IHasStorageLocation
    {
        string Path { get; }
    }

    public static class Benchmark
    {
        public static void Time(string name, Action<Stopwatch> action, IHasStorageLocation storage, bool delete = true, int records = Configuration.Transactions * Configuration.ItemsPerTransaction)
        {
            if (delete)
                DeleteDirectory(storage.Path);

            var sp = new Stopwatch();
            Console.Write("{0,-35}: running...", name);

            try
            {
                action(sp);
                Console.WriteLine("\r{0,-35}: {1,10:#,#} ms {2,10:#,#} ops / sec", name, sp.ElapsedMilliseconds, records / sp.Elapsed.TotalSeconds);
            }
            catch (NotSupportedException)
            {
                Console.WriteLine("\r{0,-35}: unsupported.", name);
            }
            catch (NotImplementedException)
            {
                Console.WriteLine("\r{0,-35}: not implemented.", name);
            }
            catch (Exception e)
            {
                Console.WriteLine("\r{0,-35}: failed with {1} exception.", name, e.GetType().Name);
            }
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
