namespace Raven.Client.Documents.Indexes.TimeSeries
{
    public class TimeSeriesIndexDefinition : IndexDefinitionBase
    {
        public override IndexSourceType SourceType => IndexSourceType.TimeSeries;
    }
}
