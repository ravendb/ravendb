namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryBuilder
    {
        ITimeSeriesQueryBuilder Raw(string queryText);

    }

    internal class TimeSeriesQueryBuilder : ITimeSeriesQueryBuilder
    {
        private string _query;
        public ITimeSeriesQueryBuilder Raw(string queryText)
        {
            _query = queryText;
            return this;
        }

        public string Query => _query;
    }

}
