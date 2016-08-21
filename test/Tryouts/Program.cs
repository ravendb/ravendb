using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;

namespace Tryouts
{
  
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var f = new SlowTests.Tests.Views.MapReduce())
                {
                    f.DoesNotOverReduce().Wait();
                }
            }

        }
    }
}

