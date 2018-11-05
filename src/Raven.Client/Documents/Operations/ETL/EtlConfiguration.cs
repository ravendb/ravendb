using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public abstract class EtlConfiguration<T> : IDatabaseTask where T : ConnectionString
    {
        private bool _initialized;

        public long TaskId { get; set; }
        
        public string Name { get; set; }

        public string MentorNode { get; set; }

        public abstract string GetDestination();

        public string ConnectionStringName { get; set; }

        internal bool TestMode { get; set; }

        [JsonDeserializationIgnore]
        [Newtonsoft.Json.JsonIgnore]
        internal T Connection { get; set; }

        public void Initialize(T connectionString)
        {
            Connection = connectionString;
            _initialized = true;
        }

        public List<Transformation> Transforms { get; set; } = new List<Transformation>();

        public bool Disabled { get; set; }
        
        public virtual bool Validate(out List<string> errors, bool validateConnection = true)
        {
            if (validateConnection && _initialized == false)
                throw new InvalidOperationException("ETL configuration must be initialized");

            errors = new List<string>();

            if (string.IsNullOrEmpty(Name))
                errors.Add($"{nameof(Name)} of ETL configuration cannot be empty");

            if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
                errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

            if (validateConnection && TestMode == false)
                Connection.Validate(ref errors);

            var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var script in Transforms)
            {
                script.Validate(ref errors, EtlType);

                if (uniqueNames.Add(script.Name) == false)
                    errors.Add($"Script name '{script.Name}' name is already defined. The script names need to be unique");
            }
            
            return errors.Count == 0;
        }

        public abstract EtlType EtlType { get; }

        public abstract bool UsingEncryptedCommunicationChannel();

        public bool AllowEtlOnNonEncryptedChannel { get; set; }

        public ulong GetTaskKey()
        {
            Debug.Assert(TaskId != 0);
            return (ulong)TaskId;
        }

        public string GetMentorNode()
        {
            return MentorNode;
        }

        public abstract string GetDefaultTaskName();

        public string GetTaskName()
        {
            return Name;
        }

        public override string ToString()
        {
            return Name;
        }

        public virtual DynamicJsonValue ToJson()
        {
            var result = new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(ConnectionStringName)] = ConnectionStringName,
                [nameof(MentorNode)] = MentorNode,
                [nameof(Transforms)] = new DynamicJsonArray(Transforms.Select(x => x.ToJson()))
            };

            return result;
        }

        public virtual bool IsEqual(EtlConfiguration<T> config)
        {
            if (config == null)
                return false;

            if (config.Transforms.Count != Transforms.Count)
                return false;

            var localTransforms = Transforms.OrderBy(x => x.Name);
            var remoteTransforms = config.Transforms.OrderBy(x => x.Name);

            using (var localEnum = localTransforms.GetEnumerator())
            using (var remoteEnum = remoteTransforms.GetEnumerator())
            {
                while(localEnum.MoveNext() && remoteEnum.MoveNext())
                {
                    if (localEnum.Current.IsEqual(remoteEnum.Current) == false)
                        return false;
                }
            }

            return config.ConnectionStringName == ConnectionStringName &&
                   config.Name == Name &&
                   config.MentorNode == MentorNode &&
                   config.Disabled == Disabled;
        }

        public bool AssertConnectionString(DatabaseRecord databaseRecord)
        {
            return EtlType == EtlType.Raven ? databaseRecord.RavenConnectionStrings.TryGetValue(ConnectionStringName, out var rvnConnection) : databaseRecord.SqlConnectionStrings.TryGetValue(ConnectionStringName, out var sqlConnection);
        }

        public static EtlType GetEtlType(BlittableJsonReaderObject etlConfiguration)
        {
            if (etlConfiguration.TryGet(nameof(EtlConfiguration<ConnectionString>.EtlType), out string type) == false)
                throw new InvalidOperationException($"ETL configuration must have {nameof(EtlConfiguration<ConnectionString>.EtlType)} field");

            if (Enum.TryParse<EtlType>(type, true, out var etlType) == false)
                throw new NotSupportedException($"Unknown ETL type: {etlType}");

            return etlType;
        }
    }
}
