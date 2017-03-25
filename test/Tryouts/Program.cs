using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Smuggler;
using System.Threading.Tasks;
using FastTests.Server.Documents.Indexing;
using Sparrow;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            //var collisions = new int[short.MaxValue+1];
            //for (int i = 0; i < short.MaxValue; i++)
            //{
            //    var str = "Field" + i;
            //    var h = OrdinalStringStructComparer.Instance.GetHashCode(str) & short.MaxValue;
            //    collisions[h]++;
            //}

            //for (int i = 0; i < collisions.Length; i++)
            //{
            //    if(collisions[i] > 6)
            //    {
            //        Console.WriteLine(i + " " + collisions[i]);
            //    }
            //}

            using (var a = new SlowTests.Blittable.BlittableJsonWriterTests.VariousPropertyAmountsTests())
            {
                a.FlatBoundarySizeFieldsAmount(maxValue: 32768);
            }
        }
    }
}