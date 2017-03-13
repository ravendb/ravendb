using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            Parallel.For(0, 100, i =>
            {
                using (var a = new FastTests.Client.LoadIntoStream())
                {
                    a.CanLoadByIdsIntoStreamUsingTransformer();
                }
            });
        }
    }
}