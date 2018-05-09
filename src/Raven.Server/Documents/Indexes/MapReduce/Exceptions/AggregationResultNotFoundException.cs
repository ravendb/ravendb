using System;

namespace Raven.Server.Documents.Indexes.MapReduce.Exceptions
{
    public class AggregationResultNotFoundException : Exception
    {
        public AggregationResultNotFoundException()
        {
        }

        public AggregationResultNotFoundException(string message) : base(message)
        {
        }

        public AggregationResultNotFoundException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
