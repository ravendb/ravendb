using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    public static IReadOnlyList<DefinitionCase> Create() =>
    [
        .. CreateMemoryExtensionsCases(),
        .. CreateBooleanArrayCases(),
        .. CreateCharArrayCases(),
        .. CreateDateTimeArrayCases(),
        .. CreateDecimalArrayCases(),
        .. CreateDoubleArrayCases(),
        .. CreateFloatArrayCases(),
        .. CreateInt32ArrayCases(),
        .. CreateInt64ArrayCases(),
        .. CreateStringArrayCases(),
        .. CreateUInt64ArrayCases()
    ];

    private static IndexDefinition Definition<TDocument, TResult>(DocumentConventions conventions, IndexDefinitionBuilder<TDocument, TResult> builder)
        => builder.ToIndexDefinition(conventions);
}
