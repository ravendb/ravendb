using FastTests.Server.Documents.Versioning;
using SlowTests.Core.AdminConsole;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var test = new AdminJsConsoleTests())
            {
                test.CanGetSettings().Wait();
            }

            using (var test = new Versioning())
            {
                test.DeleteRevisionsBeforeFromConsole(true).Wait();
            }
        }
    }
}
