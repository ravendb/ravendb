namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            int failed = 0;
            var iterations = 1000;
            var printEveryXIterations = 10;
            var sw = new Stopwatch();
            long timeRan = 0;
            long timeToDispose = 0;
            long totalTimeToRun = 0;
            long maxTimeToRun = 0;
            long minTimeToRun = long.MaxValue;
            long totalTimeToDispose = 0;
            long maxTimeToDispose = 0;
            long minTimeToDispose = long.MaxValue;
            bool enableLogging = true;
            int i = 0;
            for (; i < iterations; i++)
            {
                sw.Restart();
                if (i % printEveryXIterations == 0)
                {
                    Console.WriteLine($"starting iteration {i}");
                }
                if (enableLogging)
                {
                    LoggingSource.Instance.SetupLogMode(LogMode.Information | LogMode.Operations, @"C:\work\4.0\tests\OnNetworkDisconnection" + i);
                }
                using (var test = new ElectionTests())
                {
                    try
                    {
                        test.OnNetworkDisconnectionANewLeaderIsElectedAfterReconnectOldLeaderStepsDownAndRollBackHisLog(3).Wait();
                        timeRan = sw.ElapsedMilliseconds;
                    }
                    catch (Exception e)
                    {
                        failed++;
                        Console.WriteLine($"Failed iteration {i}{Environment.NewLine}error:{e}");
                        //Console.ReadLine();
                    }
                }
                timeToDispose = sw.ElapsedMilliseconds - timeRan;
                totalTimeToRun += timeRan;
                totalTimeToDispose += timeToDispose;
                if (timeRan > maxTimeToRun)
                    maxTimeToRun = timeRan;
                if (timeRan < minTimeToRun)
                    minTimeToRun = timeRan;
                if (timeToDispose < minTimeToDispose)
                    minTimeToDispose = timeToDispose;
                if (timeToDispose > maxTimeToDispose)
                    maxTimeToDispose = timeToDispose;
            }
            Console.WriteLine($"done with {(double)failed * 100 / iterations}% failure rate{Environment.NewLine}" +
                              $"Time Ran Avarage={(double)totalTimeToRun / iterations}ms min={minTimeToRun}ms max={maxTimeToRun}ms{Environment.NewLine}" +
                              $"Time to dispose  Avarage={(double)totalTimeToDispose / iterations}ms min={minTimeToDispose}ms max={maxTimeToDispose}ms"
            );
            Console.ReadLine();
        }
    }
}