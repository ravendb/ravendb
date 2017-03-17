using System.Collections.Generic;

namespace Raven.Server.Documents.ETL
{
    public class EtlProcessConfiguration
    {
        public string Id { get; set; } // TODO arek - to remove

        public string Name { get; set; }

        public bool Disabled { get; set; }

        public string Collection { get; set; }

        public string Script { get; set; }

        public virtual bool Validate(out List<string> errors)
        {
            errors = new List<string>();

            if (string.IsNullOrWhiteSpace(Name))
                errors.Add($"{nameof(Name)} cannot be empty");

            if (string.IsNullOrEmpty(Collection))
                errors.Add($"{nameof(Collection)} cannot be empty");

            return errors.Count == 0;
        }
    }
}