//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;

using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Client.Embedded;
using Raven.Database;
using Raven.Database.Util;
using Raven.Server;

using Xunit;

namespace Raven.Tests.Common
{
    public class RavenTest : RavenTestBase
    {
        static RavenTest()
        {
            LogManager.RegisterTarget<DatabaseMemoryTarget>();
        }

        public RavenTest()
        {
            SystemTime.UtcDateTime = () => DateTime.UtcNow;
        }

        protected void Consume(object o)
        {

        }

        protected static void EnsureDtcIsSupported(EmbeddableDocumentStore documentStore)
        {
            EnsureDtcIsSupported(documentStore.DocumentDatabase);
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
    }
}