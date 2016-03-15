using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Server.Documents.Patching;
using NetTopologySuite.Utilities;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Data;
using Raven.Client.Document;

namespace Tryouts
{
    public class Program
    {
       
        class CustomType
        {
            public string Id { get; set; }
            public string Owner { get; set; }
            public int Value { get; set; }
            public List<string> Comments { get; set; }
            public DateTime Date { get; set; }
            public DateTimeOffset DateOffset { get; set; }
        }


        private const int numOfItems = 100;

        public static void Main(string[] args)
        {
            using (var x = new AdvancedPatching())
            {
                x.CanPatchMetadata().Wait();
            }
        }
    }
}
