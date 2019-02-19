using System;
using SlowTests.Graph;

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
