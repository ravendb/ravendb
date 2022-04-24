using Raven.Client.Documents.Queries.Timings;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected QueryTimings QueryTimings = new QueryTimings();

        protected FlagHolder ShouldIncludeTimings = new FlagHolder { Value = false };

        public void IncludeTimings(out QueryTimings timings)
        {
            ShouldIncludeTimings.Value = true;
            timings = QueryTimings;
        }
    }

    public class FlagHolder
    {
        public bool Value { get; set; }
    }
}
