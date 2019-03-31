using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesBatch
    {
        public List<DocumentTimeSeriesOperation> Documents = new List<DocumentTimeSeriesOperation>();
        public bool FromEtl;
    }
}