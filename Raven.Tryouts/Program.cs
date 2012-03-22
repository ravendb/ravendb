using System;
using System.IO;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Database.Server;
using Raven.Tests.Bugs;
using Raven.Tests.Bugs.Caching;
using Raven.Tests.Shard.BlogModel;
using Raven.Tests.Queries;
using Raven.Client.Indexes;
using Raven.Client.Embedded;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tryouts
{
    internal class Program
    {
        private static void Main()
        {
            new IntersectionQuery().CanPeformIntersectionQuery_Embedded();
            new IntersectionQuery().CanPerformIntersectionQuery_Remotely();
        }
    }
}