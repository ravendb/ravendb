using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Blittable.BlittableJsonWriterTests;
using FastTests.Server.Documents;
using Raven.Tests.Core;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var blittableFormatTests = new BlittableFormatTests();

            blittableFormatTests.CheckRoundtrip("FastTests.Blittable.BlittableJsonWriterTests.Jsons.mix.json").Wait();

            foreach (var sample in BlittableFormatTests.Samples())
            {
                blittableFormatTests.CheckRoundtrip((string)sample[0]).Wait();
            }
        }
    }
}
