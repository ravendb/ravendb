using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public class Transformation
    {
        internal const string LoadTo = "loadTo";

        internal const string LoadAttachment = "loadAttachment";

        internal const string AddAttachment = "addAttachment";

        internal const string AttachmentMarker = "$attachment/";

        internal const string GenericDeleteDocumentsBehaviorFunctionKey = "$deleteDocumentsBehavior<>";

        internal const string GenericDeleteDocumentsBehaviorFunctionName = "deleteDocumentsBehavior";

        private static readonly Regex LoadToMethodRegex = new Regex($@"{LoadTo}(\w+)", RegexOptions.Compiled);

        private static readonly Regex LoadToMethodRegexAlt = new Regex($@"{LoadTo}\(\'(\w+)\'|{LoadTo}\(\""(\w+)\""", RegexOptions.Compiled);

        private static readonly Regex LoadAttachmentMethodRegex = new Regex(LoadAttachment, RegexOptions.Compiled);
        private static readonly Regex AddAttachmentMethodRegex = new Regex(AddAttachment, RegexOptions.Compiled);

        internal readonly CountersTransformation Counters;
        
        internal class CountersTransformation
        {
            internal const string Load = "loadCounter";

            internal const string Add = "addCounter";

            internal const string Marker = "$counter/";

            private static readonly Regex AddMethodRegex = new Regex(Add, RegexOptions.Compiled);

            private static readonly Regex LoadBehaviorMethodRegex = new Regex(@"function\s+loadCountersOf(\w+)Behavior\s*\(.+\)", RegexOptions.Compiled);
            
            private static readonly Regex LoadBehaviorMethodNameRegex = new Regex(@"loadCountersOf(\w+)Behavior", RegexOptions.Compiled);
            
            private readonly Transformation _parent;

            internal bool IsAddingCounters { get; private set; }
            internal Dictionary<string, string> CollectionToLoadBehaviorFunction { get; private set; }

            public CountersTransformation(Transformation parent)
            {
                _parent = parent;
            }
            
            internal void Validate(List<string> errors, EtlType type)
            {
                IsAddingCounters = AddMethodRegex.Matches(_parent.Script).Count > 0;

                if (IsAddingCounters && type == EtlType.Sql)
                    errors.Add("Adding counters isn't supported by SQL ETL");

                FillCollectionToLoadCounterBehaviorFunction(errors, type);
            }

            private void FillCollectionToLoadCounterBehaviorFunction(List<string> errors, EtlType type)
            {
                var counterBehaviors = LoadBehaviorMethodRegex.Matches(_parent.Script);
                if (counterBehaviors.Count == 0) 
                    return;
            
                if (type == EtlType.Sql)
                {
                    errors.Add("Load counter behavior functions aren't supported by SQL ETL");
                    return;
                }

                CollectionToLoadBehaviorFunction = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (Match counterBehaviorFunction in counterBehaviors)
                {
                    if (counterBehaviorFunction.Groups.Count != 2)
                    {
                        errors.Add(
                            "Invalid load counters behavior function. It is expected to have the following signature: " +
                            "loadCountersOf<CollectionName>Behavior(docId, counterName) and return 'true' if counter should be loaded to a destination");
                    }

                    var functionSignature = counterBehaviorFunction.Groups[0].Value;
                    var collection = counterBehaviorFunction.Groups[1].Value;

                    var functionName = LoadBehaviorMethodNameRegex.Match(functionSignature);

                    if (_parent.Collections.Contains(collection) == false)
                    {
                        var scriptCollections = string.Join(", ", _parent.Collections.Select(x => ($"'{x}'")));

                        errors.Add(
                            $"There is '{functionName}' function defined in '{_parent.Name}' script while the processed collections " +
                            $"({scriptCollections}) doesn't include '{collection}'. " +
                            "loadCountersOf<CollectionName>Behavior() function is meant to be defined only for counters of docs from collections that " +
                            "are loaded to the same collection on a destination side");
                    }
                    else if (_parent.GetCollectionsFromScript().Contains(collection) == false)
                    {
                        errors.Add($"`{functionName}` function where Defined while there is not load to {collection}. Load behavior function apply only if load to default collection");
                    }
                    if(CollectionToLoadBehaviorFunction.ContainsKey(collection))
                    {
                        errors.Add($"There are multiple '{functionName}' functions defined");
                    }
                    CollectionToLoadBehaviorFunction[collection] = functionName.Value;
                }
            }
        }

        internal readonly TimeSeriesTransformation TimeSeries;
        internal class TimeSeriesTransformation
        {
            internal const string Marker = "$timeSeries/";
            
            internal static class AddTimeSeries
            {
                internal const string Name = "addTimeSeries";
                public const string Signature  = "addTimeSeries(timeSeriesReference)";
                public const int ParamsCount = 1;
                internal static readonly Regex Regex = new Regex(Name, RegexOptions.Compiled);
            }
            internal static class LoadTimeSeries
            {
                public const string Name  = "loadTimeSeries";
                public const string Signature  = "loadTimeSeries(timeSeriesName, from, to)";
                public const int MinParamsCount = 1;
                public const int MaxParamsCount = 3;
            }
            
            internal static class HasTimeSeries
            {
                public const string Name  = "hasTimeSeries";
                public const string Signature  = "hasTimeSeries(timeSeriesName)";
                public const int ParamsCount = 1;
            }
            
            internal static class GetTimeSeries
            {
                public const string Name  = "getTimeSeries";
                public const string Signature  = "getTimeSeries()";
                public const int ParamsCount = 0;
            }
            
            private static class LoadTimeSeriesOfCollectionBehavior
            {
                public const string Signature  = "loadTimeSeriesOf<CollectionName>Behavior(docId, timeSeriesName)";
                public const int ParamsCount = 2;
                internal static readonly Regex Regex = new Regex(@"function\s+(?<func_name>loadTimeSeriesOf(?<collection>[A-Za-z]\w*)Behavior)\s*\(\s*((?<param>[a-zA-Z]\w*)\s*(?:,\s*(?<param>[a-zA-Z]\w*)\s*)*)?\s*\)", RegexOptions.Compiled);
            }
            private readonly Transformation _parent;

            internal bool IsAddingTimeSeries { get; private set; }
            
            internal Dictionary<string, string> CollectionToLoadBehaviorFunction { get; private set; }
            
            public TimeSeriesTransformation(Transformation parent)
            {
                _parent = parent;
            }
            
            internal void Validate(List<string> errors, EtlType type)
            {
                IsAddingTimeSeries = AddTimeSeries.Regex.Matches(_parent.Script).Count > 0;
                if (IsAddingTimeSeries && type == EtlType.Sql)
                    errors.Add("Adding time series isn't supported by SQL ETL");
                
                FillCollectionToLoadTimeSeriesBehaviorFunction(errors, type);
            }

            private void FillCollectionToLoadTimeSeriesBehaviorFunction(List<string> errors, EtlType type)
            {
                var timeSeriesBehaviors = LoadTimeSeriesOfCollectionBehavior.Regex.Matches(_parent.Script);
                if (timeSeriesBehaviors.Count == 0) 
                    return;
            
                if (type == EtlType.Sql)
                {
                    errors.Add("Load time series behavior functions aren't supported by SQL ETL");
                    return;
                }
                
                CollectionToLoadBehaviorFunction = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (Match timeSeriesBehaviorFunction in timeSeriesBehaviors)
                {
                    var functionName = timeSeriesBehaviorFunction.Groups["func_name"].Value;
                    var collection = timeSeriesBehaviorFunction.Groups["collection"].Value;
                    var args = timeSeriesBehaviorFunction.Groups["param"].Captures;

                    if (args.Count > LoadTimeSeriesOfCollectionBehavior.ParamsCount)
                    {
                        errors.Add($"'{functionName} function defined with {args.Count}. The signature should be {LoadTimeSeriesOfCollectionBehavior.Signature}");
                    }
                    if (_parent.Collections.Contains(collection) == false)
                    {
                        var scriptCollections = string.Join(", ", _parent.Collections.Select(x => ($"'{x}'")));

                        errors.Add(
                            $"There is '{functionName}' function defined in '{_parent.Name}' script while the processed collections " +
                            $"({scriptCollections}) doesn't include '{collection}'. " +
                            $"{LoadTimeSeriesOfCollectionBehavior.Signature} function is meant to be defined only for time series of docs from collections that " +
                            "are loaded to the same collection on a destination side");
                    }
                    else if (_parent.GetCollectionsFromScript().Contains(collection) == false)
                    {
                        errors.Add($"`{functionName}` function where Defined while there is not load to {collection}. Load behavior function apply only if load to default collection");
                    }
                    if(CollectionToLoadBehaviorFunction.ContainsKey(collection))
                    {
                        errors.Add($"There are multiple '{functionName}' functions defined");
                    }

                    CollectionToLoadBehaviorFunction[collection] = functionName;
                }
            }
        }
        
        private const string ParametersAndFunctionBodyRegex = @"\s*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)\s*\{(?:[^}{]+|\{(?:[^}{]+|\{[^}{]*\})*\})*\}"; // https://stackoverflow.com/questions/4204136/does-anyone-have-regular-expression-match-javascript-function

        internal static readonly Regex DeleteDocumentsBehaviorMethodRegex = new Regex(@"function\s+deleteDocumentsOf(\w+)Behavior" + ParametersAndFunctionBodyRegex, RegexOptions.Singleline);
        internal static readonly Regex DeleteDocumentsBehaviorMethodNameRegex = new Regex(@"deleteDocumentsOf(\w+)Behavior", RegexOptions.Compiled);

        internal static readonly Regex GenericDeleteDocumentsBehaviorMethodRegex = new Regex(@"function\s+" + GenericDeleteDocumentsBehaviorFunctionName + ParametersAndFunctionBodyRegex, RegexOptions.Singleline);

        private static readonly Regex Legacy_ReplicateToMethodRegex = new Regex(@"replicateTo(\w+)", RegexOptions.Compiled);

        private string[] _collections;

        public string Name { get; set; }

        public bool Disabled { get; set; }

        public List<string> Collections { get; set; } = new List<string>();

        public bool ApplyToAllDocuments { get; set; }

        public string Script { get; set; }

        internal bool IsEmptyScript { get; set; }


        internal Dictionary<string, string> CollectionToDeleteDocumentsBehaviorFunction { get; private set; }

        internal bool IsAddingAttachments { get; private set; }

        internal bool IsLoadingAttachments { get; private set; }

        public Transformation()
        {
            Counters = new CountersTransformation(this);
            TimeSeries = new TimeSeriesTransformation(this);
        }
        
        public virtual bool Validate(ref List<string> errors, EtlType type)
        {
            if (errors == null)
                throw new ArgumentNullException(nameof(errors));

            if (string.IsNullOrWhiteSpace(Name))
                errors.Add("Script name cannot be empty");

            if (ApplyToAllDocuments)
            {
                if (Collections != null && Collections.Count > 0)
                    errors.Add($"{nameof(Collections)} cannot be specified when {nameof(ApplyToAllDocuments)} is set. Script name: '{Name}'");
            }
            else
            {
                if (Collections == null || Collections.Count == 0)
                    errors.Add($"{nameof(Collections)} need be specified or {nameof(ApplyToAllDocuments)} has to be set. Script name: '{Name}'");
            }

            if (string.IsNullOrWhiteSpace(Script) == false)
            {
                if (Legacy_ReplicateToMethodRegex.Matches(Script).Count > 0)
                {
                    errors.Add($"Found `replicateTo<TableName>()` method in '{Name}' script which is not supported. " +
                               "If you are using the SQL replication script from RavenDB 3.x version then please use `loadTo<TableName>()` instead.");
                }

                IsAddingAttachments = AddAttachmentMethodRegex.Matches(Script).Count > 0;
                IsLoadingAttachments = LoadAttachmentMethodRegex.Matches(Script).Count > 0;

                Counters.Validate(errors, type);
                TimeSeries.Validate(errors, type);
                
                var deleteBehaviors = DeleteDocumentsBehaviorMethodRegex.Matches(Script);
                if (deleteBehaviors.Count > 0)
                {
                    if (type == EtlType.Sql)
                    {
                        errors.Add("Delete documents behavior functions aren't supported by SQL ETL");
                    }
                    else
                    {
                        CollectionToDeleteDocumentsBehaviorFunction = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                        for (int i = 0; i < deleteBehaviors.Count; i++)
                        {
                            var deleteBehaviorFunction = deleteBehaviors[i];

                            if (deleteBehaviorFunction.Groups.Count != 2)
                            {
                                errors.Add(
                                    "Invalid delete documents behavior function. It is expected to have the following signature: " +
                                    "deleteDocumentsOf<CollectionName>Behavior(docId) and return 'true' if document deletion should be sent to a destination");
                            }

                            var function = deleteBehaviorFunction.Groups[0].Value;
                            var collection = deleteBehaviorFunction.Groups[1].Value;

                            var functionName = DeleteDocumentsBehaviorMethodNameRegex.Match(function);

                            if (Collections.Contains(collection) == false)
                            {
                                var scriptCollections = string.Join(", ", Collections.Select(x => ($"'{x}'")));

                                errors.Add(
                                    $"There is '{functionName}' function defined in '{Name}' script while the processed collections " +
                                    $"({scriptCollections}) doesn't include '{collection}'. " +
                                    "deleteDocumentsOf<CollectionName>Behavior() function is meant to be defined only for documents from collections that " +
                                    "are loaded to the same collection on a destination side");
                            }

                            CollectionToDeleteDocumentsBehaviorFunction[collection] = functionName.Value;
                        }
                    }
                }

                var genericDeleteBehavior = GenericDeleteDocumentsBehaviorMethodRegex.Matches(Script);

                if (genericDeleteBehavior.Count > 0)
                {
                    if (type == EtlType.Sql)
                    {
                        errors.Add("Delete documents behavior functions aren't supported by SQL ETL");
                    }
                    else
                    {
                        if (genericDeleteBehavior.Count > 1)
                            errors.Add("Generic delete behavior function can be defined just once in the script");
                        else
                        {
                            if (CollectionToDeleteDocumentsBehaviorFunction == null)
                                CollectionToDeleteDocumentsBehaviorFunction = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                            CollectionToDeleteDocumentsBehaviorFunction[GenericDeleteDocumentsBehaviorFunctionKey] = GenericDeleteDocumentsBehaviorFunctionName;
                        }
                    }
                }

                var collections = GetCollectionsFromScript();

                if (collections == null || collections.Length == 0)
                {
                    var actualScript = Script;

                    if (deleteBehaviors.Count > 0)
                    {
                        // let's skip all delete behavior functions to check if we have empty transformation

                        for (int i = 0; i < deleteBehaviors.Count; i++)
                        {
                            actualScript = actualScript.Replace(deleteBehaviors[i].Value, string.Empty);
                        }
                    }

                    if (genericDeleteBehavior.Count == 1)
                    {
                        actualScript = actualScript.Replace(genericDeleteBehavior[0].Value, string.Empty);
                    }

                    if (string.IsNullOrWhiteSpace(actualScript) == false)
                    {
                        string targetName;
                        switch (type)
                        {
                            case EtlType.Raven:
                                targetName = "Collection";
                                break;
                            case EtlType.Sql:
                            case EtlType.Olap:
                                targetName = "Table";
                                break;
                            case EtlType.Elasticsearch:
                                targetName = "Index";
                                break;
                            default:
                                throw new ArgumentException($"Unknown ETL type: {type}");

                        }

                        errors.Add($"No `loadTo<{targetName}Name>()` method call found in '{Name}' script");
                    }
                    else
                    {
                        IsEmptyScript = true;
                    }
                }
            }
            else
            {
                IsEmptyScript = true;
            }

            return errors.Count == 0;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Script)] = Script,
                [nameof(Collections)] = new DynamicJsonArray(Collections),
                [nameof(ApplyToAllDocuments)] = ApplyToAllDocuments,
                [nameof(Disabled)] = Disabled
            };
        }

        internal EtlConfigurationCompareDifferences Compare(Transformation transformation)
        {
            if (transformation == null)
                throw new ArgumentNullException(nameof(transformation), "Got null transformation to compare");

            var differences = EtlConfigurationCompareDifferences.None;

            if (transformation.Collections.Count != Collections.Count)
                differences |= EtlConfigurationCompareDifferences.TransformationsCount;

            var collections = new List<string>(Collections);

            foreach (var collection in transformation.Collections)
            {
                collections.Remove(collection);
            }

            if (collections.Count != 0)
                differences |= EtlConfigurationCompareDifferences.TransformationCollectionsCount;

            if (transformation.Name.Equals(Name, StringComparison.OrdinalIgnoreCase) == false)
                differences |= EtlConfigurationCompareDifferences.TransformationName;

            if (transformation.Script != Script)
                differences |= EtlConfigurationCompareDifferences.TransformationScript;

            if (transformation.ApplyToAllDocuments != ApplyToAllDocuments)
                differences |= EtlConfigurationCompareDifferences.TransformationApplyToAllDocuments;

            if (transformation.Disabled != Disabled)
                differences |= EtlConfigurationCompareDifferences.TransformationDisabled;

            return differences;
        }

        public string[] GetCollectionsFromScript()
        {
            if (_collections != null)
                return _collections;

            var match = LoadToMethodRegex.Matches(Script);
            var matchAlt = LoadToMethodRegexAlt.Matches(Script);

            if (match.Count == 0 && matchAlt.Count == 0)
                return null;

            _collections = new string[match.Count + matchAlt.Count];

            for (var i = 0; i < match.Count; i++)
            {
                _collections[i] = match[i].Value.Substring(LoadTo.Length);
            }

            for (var i = 0; i < matchAlt.Count; i++)
            {
                var group1 = matchAlt[i].Groups[1];
                var group2 = matchAlt[i].Groups[2];
                var collection = group1.Success ? group1.Value : group2.Value;

                _collections[match.Count + i] = collection;
            }

            return _collections;
        }
    }
}
