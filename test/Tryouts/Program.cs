using System;
using System.Diagnostics;
using FastTests.Sparrow;
using FastTests.Voron.Bugs;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                var sp = Stopwatch.StartNew();
                using (var a = new SlowTests.MailingList.NullableEnums())
                {
                    a.CanQueryByNullableEnumThatIsNull();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

