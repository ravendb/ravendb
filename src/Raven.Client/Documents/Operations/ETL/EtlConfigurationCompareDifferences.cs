using System;

namespace Raven.Client.Documents.Operations.ETL
{
    [Flags]
    internal enum EtlConfigurationCompareDifferences
    {
        None,
        TransformationsCount = 1 << 1,
        TransformationCollectionsCount = 1 << 2,
        TransformationName = 1 << 3,
        TransformationScript = 1 << 4,
        TransformationApplyToAllDocuments = 1 << 5,
        TransformationDisabled = 1 << 6,
        ConnectionStringName = 1 << 7,
        ConfigurationName = 1 << 8,
        MentorNode = 1 << 9,
        ConfigurationDisabled = 1 << 10,
        TransformationDocumentIdPostfix = 1 << 11
    }
}
