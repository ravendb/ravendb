using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);

                using (var a = new FastTests.Server.Replication.ReplicationBasicTests())
                {
                    a.Master_slave_replication_with_exceptions_should_work();
                }
                
            }
        }
    }
}