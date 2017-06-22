using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Server.ETL
{
    public abstract class EtlConfiguration<T> : IDatabaseTask where T : ConnectionString
    {
        private bool _initialized;
        private long? _id;

        public string Name { get; set; }

        public abstract string GetDestination();

        public string ConnectionStringName { get; set; }

        [JsonIgnore]
        [Newtonsoft.Json.JsonIgnore]
        internal T Connection { get; set; }

        public void Initialize(T connectionString)
        {
            Connection = connectionString;
            _initialized = true;
        }

        public List<Transformation> Transforms { get; set; } = new List<Transformation>();

        public bool Disabled { get; set; }
        
        public virtual bool Validate(out List<string> errors)
        {
            if (_initialized == false)
                throw new InvalidOperationException("ETL configuration must be initialized");

            errors = new List<string>();

            if (string.IsNullOrEmpty(Name))
                errors.Add($"{nameof(Name)} of ETL configuration cannot be empty");

            if (string.IsNullOrEmpty(ConnectionStringName))
                errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

            Connection.Validate(ref errors);

            var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var script in Transforms)
            {
                script.Validate(ref errors);

                if (uniqueNames.Add(script.Name) == false)
                    errors.Add($"Script name '{script.Name}' name is already defined. The script names need to be unique");
            }
            
            return errors.Count == 0;
        }

        // TODO arek
        public long Id => _id ?? (_id = (long)Hashing.XXHash64.Calculate(Name.ToLowerInvariant(), Encodings.Utf8)).Value;

        public abstract EtlType EtlType { get; }

        public abstract bool UsingEncryptedCommunicationChannel();

        public ulong GetTaskKey()
        {
            return (ulong)Id;
        }

        public override string ToString()
        {
            return Name;
        }
    }
}