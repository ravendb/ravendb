using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Blittable;
using SlowTests.Tests.Sorting;

namespace Tryout
{
    class Program
    {
        static void Main(string[] args)
        {
            var tasks = new Func<Task>[]
            {
                async () =>
                {
                    using (var alphaNumericSorting = new AlphaNumericSorting())
                    {
                        await alphaNumericSorting.random_words_using_document_query();
                    }
                },
                async () =>
                {
                    using (var alphaNumericSorting = new AlphaNumericSorting())
                    {
                        await alphaNumericSorting.random_words();
                    }
                },
                async () =>
                {
                    using (var alphaNumericSorting = new AlphaNumericSorting())
                    {
                        await alphaNumericSorting.random_words_using_document_query_async();
                    }
                }
            };

            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                var run = new Task[tasks.Length];
                for (int j = 0; j < tasks.Length; j++)
                {
                    run[j] = Task.Run(tasks[j]);
                }
                Task.WhenAll(run);
            }

        }
    }
}