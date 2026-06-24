using System;

namespace Raven.Client.Documents.Operations.CdcSink;

[Flags]
internal enum CdcSinkConfigurationCompareDifferences
{
    None = 0,
    ConfigurationName = 1 << 0,
    ConnectionStringName = 1 << 1,
    ConnectionString = 1 << 2,
    TablesCount = 1 << 3,
    TableName = 1 << 4,
    TableDisabled = 1 << 5,
    TableConfig = 1 << 6,
    ConfigurationDisabled = 1 << 7,
    MentorNode = 1 << 8,
    PinToMentorNode = 1 << 9,
    SkipInitialLoad = 1 << 10,
    Postgres = 1 << 11,
}
