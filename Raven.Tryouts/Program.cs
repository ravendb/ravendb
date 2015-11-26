using System;
using System.Diagnostics;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main()
        {
            using (var testClass = new Tests.Raft.Client.Documents())
            {
                try
                {
                    testClass.DeleteShouldBePropagated(5);
                    Console.WriteLine("Test is done");
                }
                catch (Exception e)
                {
                    Debugger.Break();
                }
                Console.WriteLine("Dispose is done");
            }
        }
    }
}
