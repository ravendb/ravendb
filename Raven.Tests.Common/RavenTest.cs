//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Connection.Request;
using Raven.Client.Embedded;
using Raven.Database;
using Raven.Database.Util;
using Raven.Server;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.Common
{
    public class RavenTest : RavenTestBase
    {
	    public class TestMemoryTarget : DatabaseMemoryTarget
	    {
		    public override bool ShouldLog(ILog logger, LogLevel level)
		    {
			    return true;
		    }
	    }

		protected bool ShowLogs { get; set; }

        static RavenTest()
        {
			LogManager.RegisterTarget<TestMemoryTarget>();
        }

        public RavenTest()
        {
            SystemTime.UtcDateTime = () => DateTime.UtcNow;
            SystemTime.WaitCalled = null;
        }

	    public override void Dispose()
	    {
		    ShowLogsIfNecessary();

		    base.Dispose();
	    }

	    private void ShowLogsIfNecessary()
	    {
		    if (!ShowLogs)
			    return;

		    foreach (var databaseName in DatabaseNames)
		    {
				var target = LogManager.GetTarget<TestMemoryTarget>()[databaseName];
			    if (target == null)
				    continue;

			    using (var file = File.Open("debug_output.txt", FileMode.Append))
			    using (var writer = new StreamWriter(file))
			    {
					WriteLine(writer);
				    WriteLine(writer, "Logs for: " + databaseName);

				    foreach (var info in target.GeneralLog.Concat(target.WarnLog).OrderBy(x => x.TimeStamp))
				    {
						WriteLine(writer, string.Format("{0},{1},{2},{3},{4}", info.TimeStamp.ToString("yyyy-MM-dd HH:mm:ss.ffff"), info.Level, info.LoggerName, info.FormattedMessage, info.Exception));
				    }

				    WriteLine(writer);
			    }
		    }
	    }

	    private static void WriteLine(TextWriter writer, string message = "")
	    {
		    Console.WriteLine(message);
			writer.WriteLine(message);
	    }

	    protected void Consume(object o)
        {

        }

        protected static void EnsureDtcIsSupported(EmbeddableDocumentStore documentStore)
        {
            EnsureDtcIsSupported(documentStore.SystemDatabase);
        }

        protected static void EnsureDtcIsSupported(DocumentDatabase documentDatabase)
        {
            if (documentDatabase.TransactionalStorage.SupportsDtc == false)
                throw new SkipException("This test requires DTC but the storage engine " + documentDatabase.TransactionalStorage.FriendlyName + " does not support it");
        }

        protected static void EnsureDtcIsSupported(RavenDbServer server)
        {
            EnsureDtcIsSupported(server.SystemDatabase);
        }

        public double Timer(Action action)
        {
            var timer = Stopwatch.StartNew();
            action.Invoke();
            timer.Stop();
            Console.WriteLine("Time take (ms)- " + timer.Elapsed.TotalMilliseconds);
            return timer.Elapsed.TotalMilliseconds;
        }

        public static IEnumerable<object[]> Storages
        {
            get
            {
                return new[]
				{
					new object[] {"voron"},
					new object[] {"esent"}
				};
            }
        }

		protected IDocumentStoreReplicationInformer GetReplicationInformer(ServerClient client)
		{
			var replicationExecutor = client.RequestExecuter as ReplicationAwareRequestExecuter;
			return replicationExecutor == null ? null : replicationExecutor.ReplicationInformer;
		}
    }
}