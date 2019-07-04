using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class RetentionPolicy
    {
        public bool Disabled { get; set; }

        public TimeSpan? MinimumBackupAgeToKeep { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(MinimumBackupAgeToKeep)] = MinimumBackupAgeToKeep
            };
        }
    }
}
