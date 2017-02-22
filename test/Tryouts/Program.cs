using System;
using System.Linq;
using System.Threading.Tasks;
using Lucene.Net.Store;
using Sparrow.Utils;
using Voron;
using Directory = System.IO.Directory;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {

            using (var a = new SlowTests.Server.Rachis.BasicCluster())
            {
                a.ClusterWithFiveNodesAndMultipleElections().Wait();
            }
        }
    }
}