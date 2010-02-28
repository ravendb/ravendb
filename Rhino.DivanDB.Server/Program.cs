using System;

namespace Rhino.DivanDB.Server
{
    class Program
    {
        static void Main()
        {
            DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
            using (new DivanServer(@"C:\Work\divandb\Data", 8080))
            {
                Console.WriteLine("Ready to process requests...");
                Console.ReadLine();
            }
        }
    }
}
