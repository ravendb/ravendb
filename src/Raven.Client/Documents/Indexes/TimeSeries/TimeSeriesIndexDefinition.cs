namespace Raven.Client.Documents.Indexes.TimeSeries
{
    public class TimeSeriesIndexDefinition : IndexDefinition
    {
        public override IndexSourceType SourceType => IndexSourceType.TimeSeries;
    }
}
