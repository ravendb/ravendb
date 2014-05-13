// -----------------------------------------------------------------------
//  <copyright file="ReplicationBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    using Raven.Abstractions.Connection;

    public class ReplicationBehavior : ReplicationBase
    {
        private const int MaxNumber = 2048;
        private const int Quantity = 24;

        [Fact]
        public void WillAutomaticallyFailOverToSecondayServerAndThenRealizeTheServerIsUp()
        {
            var store1 = CreateStore(useFiddler:true);
            var store2 = CreateStore();

            TellFirstInstanceToReplicateToSecondInstance();

            using (var s = store1.OpenSession())
            {
                s.Store(new { Name = "Regina" }, "users/1");
                s.SaveChanges();
            }


            var replicationInformerForDatabase = store1.GetReplicationInformerForDatabase();
            replicationInformerForDatabase.DelayTimeInMiliSec = 200;
            replicationInformerForDatabase.RefreshReplicationInformation((ServerClient)store1.DatabaseCommands);

            WaitForReplication(store2, "users/1");

            using (var s = store1.OpenSession())
            {
                Assert.Equal("Regina", s.Load<dynamic>("users/1").Name);
            }

            StopDatabase(0);

            using (var s = store1.OpenSession())
            {
                Assert.Equal("Regina", s.Load<dynamic>("users/1").Name);
            }

            
            var mres = new ManualResetEventSlim();
            replicationInformerForDatabase.FailoverStatusChanged += (sender, args) => mres.Set();

            StartDatabase(0);

            Assert.False(mres.Wait(5000));
        }

        [Fact]
        public void BackoffStrategy()
        {
            var replicationInformer = new ReplicationInformer(new DocumentConvention(), new HttpJsonRequestFactory(MaxNumber))
            {
                ReplicationDestinations =
					{
						new OperationMetadata("http://localhost:2")
					}
            };

            var urlsTried = new List<Tuple<int, string>>();
            for (int i = 0; i < Quantity; i++)
            {
                var req = i + 1;
                replicationInformer.ExecuteWithReplicationAsync("GET", "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), req, 1, async url =>
                {
                    urlsTried.Add(Tuple.Create(req, url.Url));
                    if (url.Url.EndsWith("1"))
                        throw new WebException("bad", WebExceptionStatus.ConnectFailure);

                    return 1;
                }).Wait();
            }
            var expectedUrls = GetExpectedUrlForFailure().Take(urlsTried.Count).ToList();


            Assert.Equal(expectedUrls, urlsTried);
        }
     

        [Fact]
        public void BackoffStrategyWithStopStartServer()
        {
            var replicationInformer = new ReplicationInformer(new DocumentConvention(), new HttpJsonRequestFactory(MaxNumber))
            {
                ReplicationDestinations =
					{
						new OperationMetadata("http://localhost:2")
					}
            };
            int stopReq = Quantity / 2;
            int startReq = stopReq + Quantity / 4;
            var server1 = GetNewServer(8080);
            server1.Url = "http://localhost:1";
            var server2 = GetNewServer();
            server2.Url = "http://localhost:2";

            var urlsTried = new List<Tuple<int, string>>();
            for (int i = 0; i < Quantity; i++)
            {
                var req = i + 1;
                if (req == stopReq)
                {
                    StopDatabase(0);
                }
                if (req == startReq)
                {
                    StartDatabase(0);
                    server1 = servers[0];
                }
                replicationInformer.ExecuteWithReplicationAsync("GET", "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), req, 1, async url =>
                {
                    urlsTried.Add(Tuple.Create(req, url.Url));
                    if (url.Url == server1.Url && server1.DocumentStore.WasDisposed)
                        throw new WebException("Timeout! ", WebExceptionStatus.Timeout);
                    return 1;
                }).Wait();
            }
            var expectedUrls = GetExpectedUrlForStopStartFailure(stopReq, startReq).Take(urlsTried.Count).ToList();

            server1.Dispose();
            server2.Dispose();
            Assert.Equal(expectedUrls, urlsTried);
        }
        private IEnumerable<Tuple<int, string>> GetExpectedUrlForStopStartFailure(int stopReq, int startReq)
        {
            int reqCount = 1;
            for (int i = 1; i <= Quantity; i++)
            {

                if (i < stopReq)
                {
                    yield return Tuple.Create(reqCount, "http://localhost:1");
                    reqCount++;
                }
                if (i >= stopReq && i < startReq)
                {
                    yield return Tuple.Create(reqCount, "http://localhost:1");
                    yield return Tuple.Create(reqCount, "http://localhost:2");
                    reqCount++;

                }
                if (i >= startReq)
                {
                    yield return Tuple.Create(reqCount, "http://localhost:1");
                    reqCount++;
                }


            }

        }
        [Fact]
        public void ReadStriping()
        {
            var replicationInformer = new ReplicationInformer(new DocumentConvention
            {
                FailoverBehavior = FailoverBehavior.ReadFromAllServers
            }, new HttpJsonRequestFactory(MaxNumber))
            {
                ReplicationDestinations =
					{
						new OperationMetadata("http://localhost:2"),
						new OperationMetadata("http://localhost:3"),
						new OperationMetadata("http://localhost:4"),
					}
            };

            var urlsTried = new List<Tuple<int, string>>();
            for (int i = 0; i < 10; i++)
            {
                var req = i + 1;
                replicationInformer.ExecuteWithReplicationAsync("GET", "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), req, req, async url =>
                {
                    urlsTried.Add(Tuple.Create(req, url.Url));
                    return 1;
                }).Wait();
            }
            var expectedUrls = GetExpectedUrlForReadStriping().Take(urlsTried.Count).ToList();

            Assert.Equal(expectedUrls, urlsTried);
        }

        private IEnumerable<Tuple<int, string>> GetExpectedUrlForReadStriping()
        {
            int reqCount = 0;
            var urls = new[]
			{
				"http://localhost:2",
				"http://localhost:3",
				"http://localhost:4",

			};
            while (true)
            {
                reqCount++;
                var pos = reqCount % (urls.Length + 1);
                if (pos >= urls.Length)
                    yield return Tuple.Create(reqCount, "http://localhost:1");
                else
                    yield return Tuple.Create(reqCount, urls[pos]);
            }
        }

        private IEnumerable<Tuple<int, string>> GetExpectedUrlForFailure()
        {
            int reqCount = 1;
            var failCount = 0;
            // first time, we check it twice
            yield return Tuple.Create(reqCount, "http://localhost:1");
            yield return Tuple.Create(reqCount, "http://localhost:1");
            failCount++;
            yield return Tuple.Create(reqCount, "http://localhost:2");

            while (failCount < Quantity)
            {
                reqCount++;
                yield return Tuple.Create(reqCount, "http://localhost:1");
                failCount++;
                yield return Tuple.Create(reqCount, "http://localhost:2");

            }

        }
    }
}