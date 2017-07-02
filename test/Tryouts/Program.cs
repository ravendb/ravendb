using System;
using SlowTests.Bugs;
using SlowTests.Issues;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {   
                Console.WriteLine(i);
                using (var test = new RavenDB_4144())   
                {
                    test.can_use_non_existing_function3();
                }
            }
        }
    }
}
