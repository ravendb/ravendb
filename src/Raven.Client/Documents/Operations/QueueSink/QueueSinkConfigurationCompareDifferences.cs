using System;

namespace Raven.Client.Documents.Operations.QueueSink
{
    [Flags]
    internal enum QueueSinkConfigurationCompareDifferences
    {
        None,
        ScriptsCount = 1 << 1,
        TransformationCollectionsCount = 1 << 2, // queue count
        ScriptName = 1 << 3,
        Script = 1 << 4,
        ScriptDisabled = 1 << 5,
        ConnectionStringName = 1 << 6,
        ConfigurationName = 1 << 7,
        MentorNode = 1 << 8,
        ConfigurationDisabled = 1 << 9,
    }
}
