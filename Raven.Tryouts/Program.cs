using System;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main()
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var x = new RavenDB_3109())
                {
                    x.ShouldWork("voron");
        }
    }
}
    }
}
