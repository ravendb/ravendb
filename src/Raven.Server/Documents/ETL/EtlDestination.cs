using System.Collections.Generic;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlDestination
    {
        public abstract bool Validate(ref List<string> errors);

        public abstract string UniqueName { get; }

        public override string ToString()
        {
            return UniqueName;
        }
    }
}