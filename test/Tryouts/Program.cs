using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;
using SlowTests.Smuggler;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var x = new LegacySmugglerTests())
            {
                x.CanImportNorthwind("SlowTests.Smuggler.Northwind_3.5.35168.ravendbdump").Wait();
            }
        }

    }
}

