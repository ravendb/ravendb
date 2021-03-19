using Raven.Client.Documents.Indexes.Analysis;
using Raven.Server.Documents.Indexes.Analysis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Analyzers
{
    internal class PutServerWideAnalyzerCommand : PutValueCommand<AnalyzerDefinition>
    {
        public const string Prefix = "analyzer/";

        public PutServerWideAnalyzerCommand()
        {
            // for deserialization
        }

        public PutServerWideAnalyzerCommand(AnalyzerDefinition value, string uniqueRequestId)
            : base(uniqueRequestId)
        {
            if (value is null)
                throw new System.ArgumentNullException(nameof(value));

            Name = GetName(value.Name);
            Value = value;
        }

        public override void UpdateValue(ClusterOperationContext context, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.BeforeCommitFinalization += _ => AnalyzerCompilationCache.AddServerWideAnalyzer(Value);
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }

        internal static string GetName(string name)
        {
            return $"{Prefix}{name}";
        }

        public static string ExtractName(string name)
        {
            return name.Substring(Prefix.Length);
        }
    }
}
