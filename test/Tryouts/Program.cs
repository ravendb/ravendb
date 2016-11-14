using System;
using System.Diagnostics;
using FastTests.Issues;
using FastTests.Sparrow;
using FastTests.Voron.Bugs;
using Voron;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            using (var a = new FastTests.Server.Documents.SqlReplication.CanReplicate())
            {
                a.NullPropagation_WithExplicitNull().Wait();
            }
        }
    }

}

