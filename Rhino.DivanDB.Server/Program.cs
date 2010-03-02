<<<<<<< HEAD
﻿using System;

namespace Rhino.DivanDB.Server
{
    class Program
    {
        static void Main()
        {
            DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
            using (new DivanServer("Db", 8080))
            {
                Console.WriteLine("Ready to process requests...");
                Console.ReadLine();
            }
        }
    }
}
=======
﻿using System;
using System.IO;

namespace Rhino.DivanDB.Server
{
    class Program
    {
        static void Main()
        {
            DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
            Console.WriteLine(Path.GetFullPath(@"..\..\..\Data"));
            using (new DivanServer(@"..\..\..\Data", 8080))
            {
                Console.WriteLine("Ready to process requests...");
                Console.ReadLine();
            }
        }
    }
}
>>>>>>> luke
