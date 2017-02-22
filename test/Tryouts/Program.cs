using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //LoggingSource.Instance.SetupLogMode(LogMode.Information, @"C:\work\raft.log");
            //LoggingSource.Instance.EnableConsoleLogging();
            using (var basicCluster = new BasicCluster())
            {
                basicCluster.ClusterWithFiveNodesAndMultipleElections().Wait();
            }
        }
    }
}