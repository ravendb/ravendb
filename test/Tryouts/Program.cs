using System;
using System.Diagnostics;
using Raven.Client.Document;
using Raven.Client.Smuggler;
using Raven.SlowTests.Issues;

namespace Tryouts
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            using (var t = new RavenDB_2812())
            {
                t.ShouldProperlyPageResults();
            }
        }
    }
}