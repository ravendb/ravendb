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

        internal const string LoadCounter = "loadCounter";

        internal const string AddCounter = "addCounter";

        internal const string CounterMarker = "$counter/";

        internal const string GenericDeleteDocumentsBehaviorFunctionKey = "$deleteDocumentsBehavior<>";
        
        internal const string GenericDeleteDocumentsBehaviorFunctionName = "deleteDocumentsBehavior";

        private static readonly Regex LoadToMethodRegex = new Regex($@"{LoadTo}(\w+)", RegexOptions.Compiled);

        private static readonly Regex LoadAttachmentMethodRegex = new Regex(LoadAttachment, RegexOptions.Compiled);
        private static readonly Regex AddAttachmentMethodRegex = new Regex(AddAttachment, RegexOptions.Compiled);

        private static readonly Regex LoadCounterMethodRegex = new Regex(LoadCounter, RegexOptions.Compiled);
        private static readonly Regex AddCounterMethodRegex = new Regex(AddCounter, RegexOptions.Compiled);

        internal static readonly Regex LoadCountersBehaviorMethodRegex = new Regex(@"function\s+loadCountersOf(\w+)Behavior\s*\(.+\)", RegexOptions.Compiled);
        internal static readonly Regex LoadCountersBehaviorMethodNameRegex = new Regex(@"loadCountersOf(\w+)Behavior", RegexOptions.Compiled);

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

        internal Dictionary<string, string> CollectionToLoadCounterBehaviorFunction { get; private set; }

        internal Dictionary<string, string> CollectionToDeleteDocumentsBehaviorFunction { get; private set; }

        internal bool IsAddingAttachments { get; private set; }

        internal bool IsLoadingAttachments { get; private set; }

        internal bool IsAddingCounters { get; private set; }

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

                IsAddingCounters = AddCounterMethodRegex.Matches(Script).Count > 0;

                if (IsAddingCounters && type == EtlType.Sql)
                    errors.Add("Adding counters isn't supported by SQL ETL");

                var counterBehaviors = LoadCountersBehaviorMethodRegex.Matches(Script);

                if (counterBehaviors.Count > 0)
                {
                    if (type == EtlType.Sql)
                    {
                        errors.Add("Load counter behavior functions aren't supported by SQL ETL");
                    }
                    else
                    {
                        CollectionToLoadCounterBehaviorFunction = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                        for (int i = 0; i < counterBehaviors.Count; i++)
                        {
                            var counterBehaviorFunction = counterBehaviors[i];

                            if (counterBehaviorFunction.Groups.Count != 2)
                            {
                                errors.Add(
                                    "Invalid load counters behavior function. It is expected to have the following signature: " +
                                    "loadCountersOf<CollectionName>Behavior(docId, counterName) and return 'true' if counter should be loaded to a destination");
                            }

                            var functionSignature = counterBehaviorFunction.Groups[0].Value;
                            var collection = counterBehaviorFunction.Groups[1].Value;

                            var functionName = LoadCountersBehaviorMethodNameRegex.Match(functionSignature);

                            if (Collections.Contains(collection) == false)
                            {
                                var scriptCollections = string.Join(", ", Collections.Select(x => ($"'{x}'")));

                                errors.Add(
                                    $"There is '{functionName}' function defined in '{Name}' script while the processed collections " +
                                    $"({scriptCollections}) doesn't include '{collection}'. " +
                                    "loadCountersOf<CollectionName>Behavior() function is meant to be defined only for counters of docs from collections that " +
                                    "are loaded to the same collection on a destination side");
                            }

                            CollectionToLoadCounterBehaviorFunction[collection] = functionName.Value;
                        }
                    }
                }

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
                                targetName = "Table";
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

        public bool IsEqual(Transformation transformation)
        {
            if (transformation == null)
                return false;

            if (transformation.Collections.Count != Collections.Count)
                return false;

            var collections = new List<string>(Collections);

            foreach (var collection in transformation.Collections)
            {
                collections.Remove(collection);
            }

            return collections.Count == 0 &&
                   transformation.Name == Name &&
                   transformation.Script == Script &&
                   transformation.ApplyToAllDocuments == ApplyToAllDocuments &&
                   transformation.Disabled == Disabled;
        }

        public string[] GetCollectionsFromScript()
        {
            if (_collections != null)
                return _collections;

            var match = LoadToMethodRegex.Matches(Script);

            if (match.Count == 0)
                return null;

            _collections = new string[match.Count];

            for (var i = 0; i < match.Count; i++)
            {
                _collections[i] = match[i].Value.Substring(LoadTo.Length);
            }

            return _collections;
        }
    }
}
