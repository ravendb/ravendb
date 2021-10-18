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

        public virtual bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
        {
            if (validateConnection && _initialized == false)
                throw new InvalidOperationException("ETL configuration must be initialized");

            errors = new List<string>();

            if (validateName && string.IsNullOrEmpty(Name))
                errors.Add($"{nameof(Name)} of ETL configuration cannot be empty");

            if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
                errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

            if (validateConnection && TestMode == false)
                Connection.Validate(ref errors);

            var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (Transforms.Count == 0)
                throw new InvalidOperationException($"'{nameof(Transforms)}' list cannot be empty.");
            
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

        public bool IsResourceIntensive()
        {
            return false;
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
                [nameof(TaskId)] = TaskId,
                [nameof(ConnectionStringName)] = ConnectionStringName,
                [nameof(MentorNode)] = MentorNode,
                [nameof(AllowEtlOnNonEncryptedChannel)] = AllowEtlOnNonEncryptedChannel,
                [nameof(Transforms)] = new DynamicJsonArray(Transforms.Select(x => x.ToJson()))
            };

            return result;
        }

        [Obsolete("This method is not supported anymore. Will be removed in next major version of the product.")]
        public virtual bool IsEqual(EtlConfiguration<T> config)
        {
            if (config == null)
                return false;

            var result = Compare(config);
            return result == EtlConfigurationCompareDifferences.None;
        }

        internal EtlConfigurationCompareDifferences Compare(EtlConfiguration<T> config, List<(string TransformationName, EtlConfigurationCompareDifferences Difference)> transformationDiffs = null)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config), "Got null config to compare");

            var differences = EtlConfigurationCompareDifferences.None;

            if (config.Transforms.Count != Transforms.Count)
                differences |= EtlConfigurationCompareDifferences.TransformationsCount;

            var localTransforms = Transforms.OrderBy(x => x.Name);
            var remoteTransforms = config.Transforms.OrderBy(x => x.Name);

            using (var localEnum = localTransforms.GetEnumerator())
            using (var remoteEnum = remoteTransforms.GetEnumerator())
            {
                while (localEnum.MoveNext() && remoteEnum.MoveNext())
                {
                    var transformationDiff = localEnum.Current.Compare(remoteEnum.Current);
                    differences |= transformationDiff;

                    if (transformationDiff != EtlConfigurationCompareDifferences.None)
                    {
                        transformationDiffs?.Add((localEnum.Current.Name, transformationDiff));
                    }
                }
            }

            if (config.ConnectionStringName != ConnectionStringName)
                differences |= EtlConfigurationCompareDifferences.ConnectionStringName;

            if (config.Name.Equals(Name, StringComparison.OrdinalIgnoreCase) == false)
                differences |= EtlConfigurationCompareDifferences.ConfigurationName;

            if (config.MentorNode != MentorNode)
                differences |= EtlConfigurationCompareDifferences.MentorNode;

            if (config.Disabled != Disabled)
                differences |= EtlConfigurationCompareDifferences.ConfigurationDisabled;

            return differences;
        }

        [Obsolete("This method is not supported anymore. Will be removed in next major version of the product.")]
        public bool ValidateConnectionString(DatabaseRecord databaseRecord)
        {
            return EtlType == EtlType.Raven
                ? databaseRecord.RavenConnectionStrings.TryGetValue(ConnectionStringName, out _)
                : databaseRecord.SqlConnectionStrings.TryGetValue(ConnectionStringName, out _);
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
