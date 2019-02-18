using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using SlowTests.Graph;
using Sparrow.Platform;
using Xunit;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var test = new VerticesFromIndexes())
                {
                    test.Can_query_with_vertices_source_from_map_index();
                }
            }
        }
    }
}
