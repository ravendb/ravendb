namespace Raven.Client.Documents.Indexes.Counters
{
    public class CountersIndexDefinition : IndexDefinition
    {
        public override IndexSourceType SourceType => IndexSourceType.Counters;
    }
}
