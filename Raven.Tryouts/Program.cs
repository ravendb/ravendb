using System;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
    class Program
    {
        private static void Main(string[] args)
        {
            using (var test = new LargeObjectsWithJsonTextReader())
                test.MultipleAttachmentsImportShouldWork(1024 * 1024 * 500,4);
        }
    }
}