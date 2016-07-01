// -----------------------------------------------------------------------
//  <copyright file="RequestExecuterSelector.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Replication;
using Raven.Client.Document;

namespace Raven.Client.Connection.Request
{

    // Since failover behavior can change over time (ie. due to remote configuration)
    // we have to dispatch traffic to either ClusterAwareRequestExecuter or ReplicationAwareRequestExecuter
    public class RequestExecuterSelector
    {
        private readonly Func<IRequestExecuter> requestExecuterGetter;
        private readonly DocumentConvention convention;

        private IRequestExecuter clusterExecuter;
        private IRequestExecuter replicationAwareExecuter;

        public RequestExecuterSelector(Func<IRequestExecuter> requestExecuterGetter, DocumentConvention convention)
        {
            this.requestExecuterGetter = requestExecuterGetter;
            this.convention = convention;
        }

        public IRequestExecuter Select()
        {
            if (convention.FailoverBehavior == FailoverBehavior.ReadFromLeaderWriteToLeader
                || convention.FailoverBehavior == FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers
                || convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeader
                || convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers)
            {
                // use cluster aware executer
                return clusterExecuter ?? (clusterExecuter = requestExecuterGetter());
            }
            else
            {
                // use replication aware executer
                return replicationAwareExecuter ?? (replicationAwareExecuter = requestExecuterGetter());
            }
        }
    }
}