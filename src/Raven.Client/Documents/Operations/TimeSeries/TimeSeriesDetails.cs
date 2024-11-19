using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    /// <summary>
    /// Represents the result of executing a <see cref="GetMultipleTimeSeriesOperation"/>, 
    /// providing details of time series data associated with a document in RavenDB.
    /// </summary>

    public sealed class TimeSeriesDetails
    {
        /// <summary>
        /// Gets or sets the ID of the document to which the time series data belongs.
        /// </summary>
        /// <remarks>
        /// This property identifies the document that holds the time series data retrieved by the operation.
        /// </remarks>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets a dictionary containing the time series data.
        /// </summary>
        /// <remarks>
        /// The dictionary maps the name of each time series to a list of <see cref="TimeSeriesRangeResult"/> objects, 
        /// where each range result represents a segment of the time series data.
        /// The data is retrieved as part of the <see cref="GetMultipleTimeSeriesOperation"/>.
        /// </remarks>
        public Dictionary<string, List<TimeSeriesRangeResult>> Values { get; set; }
    }
}
