using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;

namespace Raven.Server.ServerWide.Maintance
{
    public class ClusterConfiguration : ConfigurationCategory
    {
        [Description("Timeout in which the node expects to recieve a heartbeat from the leader")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Cluster/ElectionTimeout")]
        public TimeSetting ElectionTimeout { get; set; }

        [Description("How freuqently we sample the information about the databases and send it to the maintenance supervisor.")]
        [DefaultValue(250)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Cluster/WorkerSamplePeriod")]
        public TimeSetting WorkerSamplePeriod { get; set; }

        [Description("As the maintenance supervisor, how freuqent we sample the information received from the nodes.")]
        [DefaultValue(500)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Cluster/SupervisorSamplePeriod")]
        public TimeSetting SupervisorSamplePeriod { get; set; }

        [Description("As the maintenance supervisor, how long we wait to hear from a worker before it is timeouted.")]
        [DefaultValue(1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Cluster/RecieveFromWorkerTimeout")]
        public TimeSetting RecieveFromWorkerTimeout { get; set; }

        [Description("As the maintenance supervisor, how long we wait after we recived an exception from a worker. Before we retry.")]
        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Cluster/OnErrorDelayTime")]
        public TimeSetting OnErrorDelayTime { get; set; }

        [Description("As a cluster node, how long it takes to timeout operation between two cluster nodes.")]
        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Cluster/ClusterOperationTimeout")]
        public TimeSetting ClusterOperationTimeout { get; set; }

    }
}
