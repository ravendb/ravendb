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