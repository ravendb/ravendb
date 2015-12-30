using Raven.Abstractions.Data;

namespace Raven.Smuggler.Counter
{
    public class CounterSmugglerOptions
    {
        public int BatchSize { get; set; }

        public bool IgnoreErrorsAndContinue { get; set; }

        public Etag StartEtag { get; set; }
    }
}