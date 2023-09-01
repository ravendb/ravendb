import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import AnalyzerDefinition = Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition;
import SorterDefinition = Raven.Client.Documents.Queries.Sorting.SorterDefinition;

export class ManageServerStubs {
    static getSampleClientGlobalConfiguration(): ClientConfiguration {
        return {
            Disabled: false,
            Etag: 103,
            IdentityPartsSeparator: ".",
            MaxNumberOfRequestsPerSession: 32,
        };
    }

    static getSampleClientDatabaseConfiguration(): ClientConfiguration {
        return {
            Disabled: false,
            Etag: 132,
            IdentityPartsSeparator: ";",
            LoadBalanceBehavior: "UseSessionContext",
            ReadBalanceBehavior: "RoundRobin",
        };
    }

    static serverWideCustomAnalyzers(): AnalyzerDefinition[] {
        return [
            { Code: "server-analyzer-code-1", Name: "First Server analyzer" },
            { Code: "server-analyzer-code-2", Name: "Second Server analyzer" },
            { Code: "server-analyzer-code-3", Name: "Third Server analyzer" },
            { Code: "server-analyzer-code-4", Name: "Fourth Server analyzer" },
        ];
    }

    static serverWideCustomSorters(): SorterDefinition[] {
        return [
            { Code: "server-sorter-code-1", Name: "First Server sorter" },
            { Code: "server-sorter-code-2", Name: "Second Server sorter" },
            { Code: "server-sorter-code-3", Name: "Third Server sorter" },
            { Code: "server-sorter-code-4", Name: "Fourth Server sorter" },
        ];
    }
}
