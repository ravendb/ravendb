namespace Raven.Client.Documents.Indexes.Counters
{
    public sealed class CountersIndexDefinition : IndexDefinition
    {
        public override IndexSourceType SourceType => IndexSourceType.Counters;
    }
}
