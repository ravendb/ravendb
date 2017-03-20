using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Jint;
using Jint.Native;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlDocumentTransformer : EtlTransformer<RavenEtlItem, ICommandData>
    {
        private readonly RavenEtlConfiguration _configuration;
        

        private readonly List<ICommandData> _commands = new List<ICommandData>();
        private readonly PatchRequest _transformationScript;

        private RavenEtlItem _currentlyTransformed;

        public RavenEtlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, RavenEtlConfiguration configuration) : base(database, context)
        {
            _configuration = configuration;

            if (string.IsNullOrEmpty(configuration.Script))
            {
                LoadToDestinations = new string[0];
                return;
            }

            _transformationScript = new PatchRequest { Script = configuration.Script };

            var collections = configuration.GetCollectionsFromScript();

            if (collections.Length > 1)
            {
                IsLoadingToMultipleCollections = true;

                IsLoadingToDefaultCollection = false;
                // ReSharper disable once ForCanBeConvertedToForeach
                for (var i = 0; i < collections.Length; i++)
                {
                    if (collections[i].Equals(configuration.Collection, StringComparison.OrdinalIgnoreCase))
                    {
                        IsLoadingToDefaultCollection = true;
                        break;
                    }
                }
            }
            else if (collections.Length == 1 && collections[0].Equals(configuration.Script, StringComparison.OrdinalIgnoreCase) == false)
                IsLoadingToDefaultCollection = false;

            LoadToDestinations = collections;
        }

        private bool IsLoadingToDefaultCollection { get; } = true;

        private bool IsLoadingToMultipleCollections { get; }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string collectionName, JsValue document, PatcherOperationScope scope)
        {
            if (collectionName == null)
                throw new ArgumentException("collectionName parameter is mandatory");
            if (document == null)
                throw new ArgumentException("document parameter is mandatory");

            var transformed = scope.ToBlittable(document.AsObject());

            string customId = null;
            if (IsLoadingToDefaultCollection == false || collectionName.Equals(_configuration.Collection, StringComparison.OrdinalIgnoreCase) == false)
            {
                DynamicJsonValue metadata;

                if (transformed[Constants.Documents.Metadata.Key] != null)
                    metadata = transformed[Constants.Documents.Metadata.Key] as DynamicJsonValue;
                else
                    transformed[Constants.Documents.Metadata.Key] = metadata = new DynamicJsonValue();

                customId = $"{_currentlyTransformed.DocumentKey}/{collectionName.ToLowerInvariant()}/";

                metadata[Constants.Documents.Metadata.Collection] = collectionName;
                metadata[Constants.Documents.Metadata.Id] = customId;
            }

            var id = customId ?? _currentlyTransformed.DocumentKey;

            var transformResult = Context.ReadObject(transformed, id);

            _commands.Add(new PutCommandDataWithBlittableJson(id, null, transformResult));
        }

        public override IEnumerable<ICommandData> GetTransformedResults()
        {
            return _commands;
        }

        public override void Transform(RavenEtlItem item)
        {
            if (item.IsDelete)
            {
                if (IsLoadingToDefaultCollection)
                    _commands.Add(new DeleteCommandData(item.DocumentKey, null));

                if (IsLoadingToMultipleCollections)
                {
                    // TODO arek
                }
            }
            else
            {
                if (_transformationScript != null)
                {
                    _currentlyTransformed = item;

                    Apply(Context, _currentlyTransformed.Document, _transformationScript);
                }
                else
                    _commands.Add(new PutCommandDataWithBlittableJson(item.DocumentKey, null, item.Document.Data));
            }
        }
    }
}