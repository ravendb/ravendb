namespace Raven.Client.Documents.Indexes.TimeSeries
{
    public sealed class TimeSeriesIndexDefinition : IndexDefinition
    {
        public override IndexSourceType SourceType => IndexSourceType.TimeSeries;
    }
}
