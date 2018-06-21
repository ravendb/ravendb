using Raven.Client.Documents.Queries.Timings;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected QueryTimings QueryTimings;

        public void IncludeTimings(out QueryTimings timings)
        {
            if (QueryTimings != null)
            {
                timings = QueryTimings;
                return;
            }

            QueryTimings = timings = new QueryTimings();
        }
    }
}
