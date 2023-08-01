using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public sealed class LicenseRenewalResult
    {
        public string SentToEmail  { get; set; }

        public DateTime? NewExpirationDate { get; set; }
        
        public string Error { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(SentToEmail)] = SentToEmail,
                [nameof(NewExpirationDate)] = NewExpirationDate,
                [nameof(Error)] = Error
            };
        }
    }
}
