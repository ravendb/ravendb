namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var basicCluster = new BasicCluster())
            {
                basicCluster.CanSetupSingleNode().Wait();
            }
        }
    }
}