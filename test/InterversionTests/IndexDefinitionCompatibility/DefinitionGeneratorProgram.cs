using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

namespace InterversionTests.IndexDefinitionCompatibility;

internal static class DefinitionGeneratorProgram
{
    public static GeneratorOutput Generate()
    {
        var generatorAssembly = Assembly.GetExecutingAssembly();
        var clientPath = typeof(IndexDefinition).Assembly.Location;

        return new GeneratorOutput
        {
            CompilerFingerprint = GetAssemblyMetadata(generatorAssembly, "CompilerFingerprint"),
            RavenClientProductVersion = FileVersionInfo.GetVersionInfo(clientPath).ProductVersion,
            Definitions = GenerateDefinitions(GetDefaultConventions())
        };
    }

    private static Dictionary<string, DefinitionText> GenerateDefinitions(DocumentConventions conventions)
    {
        var definitions = new Dictionary<string, DefinitionText>(StringComparer.Ordinal);
        AddDefinitions(definitions, "operation/", GeneratedDefinitionCases.Create(), conventions);
        AddDefinitions(definitions, "topology/", StructuralDefinitionCases.Create(), conventions);

        return definitions;
    }

    private static void AddDefinitions(Dictionary<string, DefinitionText> definitions, string caseIdPrefix, IEnumerable<DefinitionCase> cases, DocumentConventions conventions)
    {
        foreach (var definitionCase in cases.OrderBy(x => x.CaseId, StringComparer.Ordinal))
        {
            var caseId = caseIdPrefix + definitionCase.CaseId;
            try
            {
                var definition = definitionCase.Build(conventions);
                var text = new DefinitionText
                {
                    Maps = [.. definition.Maps.OrderBy(x => x, StringComparer.Ordinal)],
                    Reduce = definition.Reduce
                };

                if (definitions.TryAdd(caseId, text) == false)
                    throw new InvalidOperationException($"Duplicate CaseId: {caseId}");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to generate '{caseId}'.", e);
            }
        }
    }

    private static DocumentConventions GetDefaultConventions()
    {
        var field = typeof(DocumentConventions).GetField("Default", BindingFlags.Static | BindingFlags.NonPublic);
        return field?.GetValue(null) as DocumentConventions
               ?? throw new InvalidOperationException("Raven.Client internal DocumentConventions.Default field was not found.");
    }

    private static string GetAssemblyMetadata(Assembly assembly, string key)
    {
        var metadata = assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>()
            .SingleOrDefault(x => string.Equals(x.Key, key, StringComparison.Ordinal));

        return metadata?.Value
               ?? throw new InvalidOperationException($"Assembly metadata '{key}' was not found.");
    }
}
