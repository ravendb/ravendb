// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2176.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;

using Raven.Abstractions.Logging;
using Raven.Database.Server;
using Raven.Database.Server.Connections;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2176 : RavenTest
    {
        [Fact]
        public void CanAddNewClient()
        {
            using (NewDocumentStore())
            {
                var target = LogManager.GetTarget<AdminLogsTarget>();
                Assert.NotNull(target);

                using (var client1Target = target.For("client1"))
                {
                    var fakeTransport1 = new FakeLogTransport();
                    client1Target.Reconnect(fakeTransport1);
                    var category1Log = LogManager.GetLogger("Raven.Category1");

                    client1Target.EnableLogging("Raven.Category1", LogLevel.Info);

                    category1Log.Debug("Debug");
                    category1Log.Info("Info");
                    category1Log.Error("Error");

                    Assert.Equal(2, fakeTransport1.Messages.Count);
                    Assert.Equal("Info", fakeTransport1.Messages.Take().FormattedMessage);
                    Assert.Equal("Error", fakeTransport1.Messages.Take().FormattedMessage);
                }
            }
        }

        [Fact]
        public void CanHandleLogCategoryShading()
        {
            using (NewDocumentStore())
            {
                var target = LogManager.GetTarget<AdminLogsTarget>();
                Assert.NotNull(target);

                using (var client1Target = target.For("client1"))
                {
                    var fakeTransport1 = new FakeLogTransport();
                    client1Target.Reconnect(fakeTransport1);
                    var category1Log = LogManager.GetLogger("Raven.Category1");
                    var category1Sub = LogManager.GetLogger("Raven.Category1.Sub1");

                    client1Target.EnableLogging("Raven.Category1", LogLevel.Info);
                    client1Target.EnableLogging("Raven.Category1.Sub1", LogLevel.Debug);

                    category1Log.Debug("Debug1");
                    category1Log.Info("Info1");
                    category1Log.Error("Error1");

                    category1Sub.Debug("Debug2");
                    category1Sub.Info("Info2");
                    category1Sub.Error("Error2");


                    Assert.Equal(5, fakeTransport1.Messages.Count);
                    Assert.Equal("Info1", fakeTransport1.Messages.Take().FormattedMessage);
                    Assert.Equal("Error1", fakeTransport1.Messages.Take().FormattedMessage);
                    Assert.Equal("Debug2", fakeTransport1.Messages.Take().FormattedMessage);
                    Assert.Equal("Info2", fakeTransport1.Messages.Take().FormattedMessage);
                    Assert.Equal("Error2", fakeTransport1.Messages.Take().FormattedMessage);
                }
            }
        }
    }

    class FakeLogTransport : IEventsTransport
    {
        public BlockingCollection<LogEventInfo> Messages { get; private set; }

        public FakeLogTransport()
        {
             Messages = new BlockingCollection<LogEventInfo>();
            Connected = true;
        }

        public void Dispose()
        {
        }

        public string Id { get; private set; }

        public bool Connected { get; set; }

#pragma warning disable 67
		public event Action Disconnected;
#pragma warning restore 67
        
        public void SendAsync(object msg)
        {
            var message = msg as LogEventInfo;
            Messages.Add(message);
        }
        
        public string ResourceName{get;set;}
        public long CoolDownWithDataLossInMiliseconds{get;set;}
        
    }
}