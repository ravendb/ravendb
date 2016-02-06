using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Blittable;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            new ArrayParsingTests().CanParseSimpleArray().Wait();
        }
    }
}
