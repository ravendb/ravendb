using System;
using System.Runtime.CompilerServices;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Sparrow.Utils;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly ShardingTestBase Sharding;

    public class ShardingTestBase
    {
        private readonly RavenTestBase _parent;

        public ShardingTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public IDocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            options ??= new Options();
            options.ModifyDatabaseRecord += r =>
            {
                r.Shards = new[]
                {
                    new DatabaseTopology(),
                    new DatabaseTopology(),
                    new DatabaseTopology(),
                };
            };

            options.ModifyDocumentStore = s => s.Conventions.OperationStatusFetchMode = OperationStatusFetchMode.Polling;
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "remove above after changes api is working");

            return _parent.GetDocumentStore(options, caller);
        }
    }
}

