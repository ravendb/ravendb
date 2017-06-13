using System;
using FastTests.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {

            for (var i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new RavenDB_7059())
                {
                    test.Cluster_identity_should_work_with_smuggler().Wait();
                }
            }
        }
    }

}
