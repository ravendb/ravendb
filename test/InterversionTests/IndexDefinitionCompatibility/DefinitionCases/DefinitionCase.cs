using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal sealed class DefinitionCase
{
    public DefinitionCase(string caseId, Func<DocumentConventions, IndexDefinition> build)
    {
        CaseId = caseId;
        Build = build;
    }

    public string CaseId { get; }
    public Func<DocumentConventions, IndexDefinition> Build { get; }
}
