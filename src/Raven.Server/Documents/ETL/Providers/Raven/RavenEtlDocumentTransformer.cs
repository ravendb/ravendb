using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
// ReSharper disable ForCanBeConvertedToForeach

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlDocumentTransformer : EtlTransformer<RavenEtlItem, ICommandData>
    {
        private readonly ScriptInput _script;
        private readonly List<ICommandData> _commands = new List<ICommandData>();

        private RavenEtlItem _currentlyTransformed;

        public RavenEtlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, ScriptInput script) : base(database, context)
        {
            _script = script;

            LoadToDestinations = _script.Transformation == null ? new string[0] : _script.AllCollections;
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string collectionName, JsValue document, PatcherOperationScope scope)
        {
            if (collectionName == null)
                ThrowLoadParameterIsMandatory(nameof(collectionName));
            if (document == null)
                ThrowLoadParameterIsMandatory(nameof(document));

            var transformed = scope.ToBlittable(document.AsObject());

            string prefixedId = null;

            if (_script.IsLoadingToDefaultCollection == false ||
                _script.NonDefaultCollections.Length > 0 && 
                _script.DefaultCollection.Equals(collectionName, StringComparison.OrdinalIgnoreCase) == false)
            {
                DynamicJsonValue metadata;

                if (transformed[Constants.Documents.Metadata.Key] != null)
                    metadata = transformed[Constants.Documents.Metadata.Key] as DynamicJsonValue;
                else
                    transformed[Constants.Documents.Metadata.Key] = metadata = new DynamicJsonValue();

                prefixedId = GetPrefixedId(_currentlyTransformed.DocumentKey, collectionName);

                metadata[Constants.Documents.Metadata.Collection] = collectionName;
                metadata[Constants.Documents.Metadata.Id] = prefixedId;
            }

            var id = prefixedId ?? _currentlyTransformed.DocumentKey;

            var transformResult = Context.ReadObject(transformed, id);

            _commands.Add(new PutCommandDataWithBlittableJson(id, null, transformResult));
        }

        private string GetPrefixedId(string documentId, string loadCollectionName)
        {
            return $"{documentId}/{_script.IdPrefixForCollection[loadCollectionName]}/";
        }

        public override IEnumerable<ICommandData> GetTransformedResults()
        {
            return _commands;
        }

        public override void Transform(RavenEtlItem item)
        {
            if (item.IsDelete)
            {
                if (_script.IsLoadingToDefaultCollection)
                    _commands.Add(new DeleteCommandData(item.DocumentKey, null));

                if (_script.NonDefaultCollections.Length > 0)
                {
                    for (var i = 0; i < _script.NonDefaultCollections.Length; i++)
                    {
                        _commands.Add(new DeleteCommandData(GetPrefixedId(item.DocumentKey, _script.NonDefaultCollections[i]), null));
                    }
                }
            }
            else
            {
                if (_script.Transformation != null)
                {
                    if (_script.NonDefaultCollections.Length > 0 && item.Document.ChangeVector.Length > 1)
                    {
                        // we need to delete all docs prefixed by modified document key to properly handle updates

                        for (var i = 0; i < _script.NonDefaultCollections.Length; i++)
                        {
                            _commands.Add(new DeleteCommandData(GetPrefixedId(item.DocumentKey, _script.NonDefaultCollections[i]), null));
                        }
                    }

                    _currentlyTransformed = item;

                    Apply(Context, _currentlyTransformed.Document, _script.Transformation);
                }
                else
                    _commands.Add(new PutCommandDataWithBlittableJson(item.DocumentKey, null, item.Document.Data));
            }
        }

        public class ScriptInput
        {
            public readonly string[] AllCollections = new string[0];

            public readonly string[] NonDefaultCollections = new string[0];

            public readonly bool IsLoadingToDefaultCollection = true;

            public readonly PatchRequest Transformation;

            public readonly string DefaultCollection;

            public readonly Dictionary<string, string> IdPrefixForCollection = new Dictionary<string, string>();

            public ScriptInput(RavenEtlConfiguration configuration)
            {
                DefaultCollection = configuration.Collection;

                if (string.IsNullOrEmpty(configuration.Script))
                {
                    return;
                }

                Transformation = new PatchRequest { Script = configuration.Script };

                AllCollections = configuration.GetCollectionsFromScript();

                if (AllCollections.Length > 1)
                {
                    IsLoadingToDefaultCollection = false;

                    var nonDefault = new List<string>();

                    for (var i = 0; i < AllCollections.Length; i++)
                    {
                        if (AllCollections[i].Equals(DefaultCollection, StringComparison.OrdinalIgnoreCase))
                        {
                            IsLoadingToDefaultCollection = true;
                        }
                        else
                        {
                            nonDefault.Add(AllCollections[i]);
                        }
                    }

                    NonDefaultCollections = nonDefault.ToArray();
                }
                else if (AllCollections.Length == 1 && AllCollections[0].Equals(DefaultCollection, StringComparison.OrdinalIgnoreCase) == false)
                    IsLoadingToDefaultCollection = false;

                foreach (var collection in NonDefaultCollections)
                {
                    IdPrefixForCollection[collection] = DocumentConventions.DefaultTransformCollectionNameToDocumentIdPrefix(collection);
                }
            }
        }
    }
}