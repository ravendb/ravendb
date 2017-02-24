using System;
using System.Diagnostics;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {

            using (var a = new SlowTests.Core.Commands.Indexes())
            {
                a.CanResetIndex().Wait();
            }
        }
    }
}