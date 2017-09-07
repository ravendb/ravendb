using System;
using System.Threading.Tasks;
using SlowTests.Issues;
using SlowTests.Server.Documents.PeriodicBackup;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var test = new RavenDB_7136())
                {
                    test.IfOneOfTheMultiMapFunctionsIsFailingWeNeedToResetTheEnumeratorToAvoidApplyingWrongFunctionOnPreviousDocument();
                }
            }
        }
    }
}
