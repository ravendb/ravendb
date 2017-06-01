using System.Collections.Generic;

namespace Raven.Client.Server.ETL
{
    public abstract class EtlDestination
    {
        public abstract bool Validate(ref List<string> errors);

        public abstract string Name { get; }

        public abstract bool UsingEncryptedCommunicationChannel();

        public override string ToString()
        {
            return Name;
        }
    }
}