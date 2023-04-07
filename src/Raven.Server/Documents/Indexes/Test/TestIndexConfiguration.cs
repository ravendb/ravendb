using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;

namespace Raven.Server.Documents.Indexes.Test;

public class TestIndexConfiguration : SingleIndexConfiguration
{
    public TestIndexConfiguration(IndexConfiguration clientConfiguration, RavenConfiguration databaseConfiguration) : base(clientConfiguration, databaseConfiguration)
    {
    }

    public override bool RunInMemory => true;
}
