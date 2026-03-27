using System;

namespace Raven.Client.Documents.Operations.CdcSink;

[Flags]
internal enum CdcSinkConfigurationCompareDifferences
{
    None = 0,
    ConfigurationName = 1 << 0,
    ConnectionStringName = 1 << 1,
    ConnectionString = 1 << 2,
    ScriptsCount = 1 << 3,
    ScriptName = 1 << 4,
    Script = 1 << 5,
    ScriptDisabled = 1 << 6,
    ConfigurationDisabled = 1 << 7,
    MentorNode = 1 << 8,
}
