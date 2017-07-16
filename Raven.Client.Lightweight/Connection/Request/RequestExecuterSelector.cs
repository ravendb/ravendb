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

        private readonly bool avoidCluster;
        public RequestExecuterSelector(Func<IRequestExecuter> requestExecuterGetter, DocumentConvention convention, bool avoidCluster = false)
        {
            this.requestExecuterGetter = requestExecuterGetter;
            this.convention = convention;
            this.avoidCluster = avoidCluster;
        }

        public IRequestExecuter Select()
        {
            if (avoidCluster == false &&
                (convention.FailoverBehavior == FailoverBehavior.ReadFromLeaderWriteToLeader
                || convention.FailoverBehavior == FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers
                || convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeader
                || convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers
                )
               )                
            {
                // use cluster aware executer
                return clusterExecuter ?? (clusterExecuter = requestExecuterGetter());
            }
            // use replication aware executer
            return replicationAwareExecuter ?? (replicationAwareExecuter = requestExecuterGetter());
        }
    }
}