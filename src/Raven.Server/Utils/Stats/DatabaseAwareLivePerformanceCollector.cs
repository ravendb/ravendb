using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Utils.Stats
{
    public abstract class DatabaseAwareLivePerformanceCollector<T> : LivePerformanceCollector<T>
    {
        protected readonly DocumentDatabase Database;

        protected DatabaseAwareLivePerformanceCollector(DocumentDatabase database): base(database.DatabaseShutdown, database.Name)
        {
            Database = database;
        }
        
    }
}
