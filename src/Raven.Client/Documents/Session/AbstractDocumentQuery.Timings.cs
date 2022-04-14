using Raven.Client.Documents.Queries.Timings;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected QueryTimings QueryTimings;

        protected FlagHolder ShouldIncludeTimings = new FlagHolder { Value = false };

        public void IncludeTimings(out QueryTimings timings)
        {
            if (QueryTimings == null)
            {
                QueryTimings = new QueryTimings();
            }
            timings = QueryTimings;

            ShouldIncludeTimings.Value = true;
        }
    }

    public class FlagHolder
    {
        public bool Value { get; set; }
    }
}
