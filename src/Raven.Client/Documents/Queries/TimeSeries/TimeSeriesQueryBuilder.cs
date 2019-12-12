namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryBuilder
    {
        T Raw<T>(string queryText) where T : TimeSeriesQueryResult;
    }

    internal class TimeSeriesQueryBuilder : ITimeSeriesQueryBuilder
    {
        private string _query;
        public T Raw<T>(string queryText) where T : TimeSeriesQueryResult
        {
            _query = queryText;
            return default;
        }

        public string Query => _query;
    }

}
