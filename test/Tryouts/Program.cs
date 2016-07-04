using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using FastTests.Blittable;
using SlowTests.Tests.Sorting;

namespace Tryout
{
    class Program
    {
        static void Main(string[] args)
        {
            new AlphaNumericSorting().random_words_using_document_query_async().Wait();
        }
    }
}