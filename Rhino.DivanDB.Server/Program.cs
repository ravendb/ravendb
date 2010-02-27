using System;

namespace Rhino.DivanDB.Server
{
    class Program
    {
        static void Main()
        {
            using (new DivanServer("Db", 8080))
            {
                Console.WriteLine("Ready to process requests...");
                Console.ReadLine();
            }
        }
    }
}
