using SlowTests.Server.Documents.PeriodicBackup;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var test = new PeriodicBackupTestsSlow())
            {
                 test.CanImportTombstonesFromIncrementalBackup().Wait();
            }
        }
    }
}
