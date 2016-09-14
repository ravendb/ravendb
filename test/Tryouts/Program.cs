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
            using (var x = new FastTests.Server.Documents.Versioning.Versioning())
            {
                x.WillDeleteRevisionsIfDeleted_OnlyIfPurgeOnDeleteIsTrue().Wait();
            }
        }

    }
}

