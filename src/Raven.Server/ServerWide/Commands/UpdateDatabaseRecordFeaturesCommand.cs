using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    // Base for all UpdateDatabaseCommand types that represent features.
    // Used to handle license-limited features and allow skipping license
    // asserts when a feature is being disabled.
    public abstract class UpdateDatabaseRecordFeaturesCommand : UpdateDatabaseCommand
    {
        protected UpdateDatabaseRecordFeaturesCommand() { }

        protected UpdateDatabaseRecordFeaturesCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId) { }

        public abstract bool Disabled { get; }

        public override void AssertBeforeAppend(ClusterOperationContext context, ServerStore serverStore, long index)
        {
            AssertLicenseLimitsOnUpdatedRecord(context, serverStore, index);
        }
    }
}
