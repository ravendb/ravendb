using System;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                
                Console.WriteLine(i);
                using (var a = new FastTests.Voron.MultiValueTree())
                {
                    a.MultiDelete_Remains_One_Entry_The_Data_Is_Retrieved_With_MultiRead();
                }
            }
        }
    }

}

