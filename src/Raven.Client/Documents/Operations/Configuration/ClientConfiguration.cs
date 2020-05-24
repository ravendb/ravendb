using System;
using Raven.Client.Http;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Configuration
{
    public class ClientConfiguration
    {
        private char? _identityPartsSeparator;

        public long Etag { get; set; }

        public bool Disabled { get; set; }

        public int? MaxNumberOfRequestsPerSession { get; set; }

        public ReadBalanceBehavior? ReadBalanceBehavior { get; set; }
        
        public WriteBalanceBehavior? WriteBalanceBehavior { get; set; }

        public char? IdentityPartsSeparator
        {
            get => _identityPartsSeparator;
            set
            {
                if (value == '|')
                    throw new InvalidOperationException("Cannot set identity parts separator to '|'.");

                _identityPartsSeparator = value;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(Etag)] = Etag,
                [nameof(MaxNumberOfRequestsPerSession)] = MaxNumberOfRequestsPerSession,
                [nameof(ReadBalanceBehavior)] = ReadBalanceBehavior,
                [nameof(WriteBalanceBehavior)] = WriteBalanceBehavior,
                [nameof(IdentityPartsSeparator)] = IdentityPartsSeparator
            };
        }
    }
}
