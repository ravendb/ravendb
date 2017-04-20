using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL
{
    public class EtlConfiguration<T> where T : EtlDestination
    {
        public T Destination { get; set; }

        public List<Transformation> Transforms { get; set; } = new List<Transformation>();
        
        public virtual bool Validate(out List<string> errors)
        {
            errors = new List<string>();

            Destination.Validate(ref errors);

            var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var script in Transforms)
            {
                script.Validate(ref errors);

                if (uniqueNames.Add(script.Name) == false)
                    errors.Add($"Script name '{script.Name}' name is already defined. The script names need to be unique");
            }
            
            return errors.Count == 0;
        }
    }
}