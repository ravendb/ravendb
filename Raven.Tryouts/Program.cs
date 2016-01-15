using System;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
    class Program
    {
        private static void Main(string[] args)
        {
            using (var test = new RavenDB_4103())
            {
                test.DeleteConflitDocumentsFirstMainAfter();
            }
        }
    }
}