// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4748.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4748 : ReplicationBase
    {

        [Fact]
        public void should_refuse_to_replicate_when_changing_from_replication_to_etl()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                SetupReplication(source.DatabaseCommands, destination);

                using (var session = source.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                WaitForReplication(destination, "users/1");

                SetupReplication(source.DatabaseCommands, new Dictionary<string, string>
                {
                    { "users", null }
                }, destination);

                using (var session = source.OpenSession())
                {
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }

                Assert.True(SpinWait.SpinUntil(() =>
                {
                    var sourceJson = source.DatabaseCommands.StartsWith(Constants.RavenReplicationDestinationsBasePath, null, 0, 1).FirstOrDefault();
                    if (sourceJson == null)
                        return false;
                    var replicationInfo = sourceJson.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
                    return replicationInfo.FailureCount > 0;
                }, TimeSpan.FromSeconds(4)));
            }
        }

        [Fact]
        public void should_refuse_to_replicate_when_changing_from_etl_toreplication()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                SetupReplication(source.DatabaseCommands, new Dictionary<string, string>
                {
                    { "users", null }
                }, destination);
                
                using (var session = source.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                WaitForReplication(destination, "users/1");

                SetupReplication(source.DatabaseCommands, destination);

                using (var session = source.OpenSession())
                {
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }

                Assert.True(SpinWait.SpinUntil(() =>
                {
                    var sourceJson = source.DatabaseCommands.StartsWith(Constants.RavenReplicationDestinationsBasePath, null, 0, 1).FirstOrDefault();
                    if (sourceJson == null)
                        return false;
                    var replicationInfo = sourceJson.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
                    return replicationInfo.FailureCount > 0;
                }, TimeSpan.FromSeconds(4)));
            }
        }
    }
}