using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var testclass = new SlowTests.Server.Rachis.Cluster())
            {
                testclass.CanCreateDatabaseWithReplicationFactorLowerThanTheClusterSize().Wait();
            }
        }
    }
}