// -----------------------------------------------------------------------
//  <copyright file="ReplicationBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;

using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
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
                replicationInformer.ExecuteWithReplicationAsync<int>(HttpMethods.Get, "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), null, req, req, url =>
                {
	                urlsTried.Add(Tuple.Create(req, url.Url));
	                return new CompletedTask<int>(1);
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

    }
}