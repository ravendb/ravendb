using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;

namespace Raven.Server.ServerWide.Maintance
{
    public class ClusterMaintainceConfiguration : ConfigurationCategory
    {
        [Description("How freuqent we sample the information about the databases and send it to the leader.")]
        [DefaultValue(250)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/ClusterMaintaince/NodeSamplePeriod")]
        public TimeSetting NodeSamplePeriod { get; set; }

        [Description("As the leader, how freuqent we sample the information received from the nodes.")]
        [DefaultValue(500)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/ClusterMaintaince/LeaderSamplePeriod")]
        public TimeSetting LeaderSamplePeriod { get; set; }

        [Description("As the leader, how long we wait to hear from a node before it is timeouted.")]
        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/ClusterMaintaince/RecieveFromNodeTimeout")]
        public TimeSetting RecieveFromNodeTimeout { get; set; }

        [Description("As the leader, how long we wait after we recived an exception from a node. Before we retry.")]
        [DefaultValue(1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/ClusterMaintaince/OnErrorDelayTime")]
        public TimeSetting OnErrorDelayTime { get; set; }

        [Description("Maximal allowed gap between the etag and the indexed etag of the node, in order to be promotted.")]
        [DefaultValue(1000)]
        [ConfigurationEntry("Raven/ClusterMaintaince/MaxIndexEtagInterval")]
        public long MaxIndexEtagInterval { get; set; }

    }
}
