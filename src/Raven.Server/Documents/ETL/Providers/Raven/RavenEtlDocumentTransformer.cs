﻿using System;
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
        private static readonly Regex LoadToMethodRegex = new Regex($@"{LoadTo}(\w+)", RegexOptions.Compiled);

        private readonly List<ICommandData> _commands = new List<ICommandData>();
        private readonly PatchRequest _transformationScript;

        private RavenEtlItem _currentlyTransformed;

        public RavenEtlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, RavenEtlConfiguration configuration) : base(database, context)
        {
            _configuration = configuration;

            if (string.IsNullOrEmpty(configuration.Script) == false)
            {
                var match = LoadToMethodRegex.Matches(configuration.Script);

                if (match.Count > 0)
                {
                    LoadToDestinations = new string[match.Count];

                    for (var i = 0; i < match.Count; i++)
                    {
                        LoadToDestinations[i] = match[i].Value.Substring(LoadTo.Length);
                    }
                }

                _transformationScript = new PatchRequest { Script = configuration.Script };
            }
        }

        protected override void LoadToFunction(string collectionName, JsValue document, PatcherOperationScope scope)
        {
            if (collectionName == null)
                throw new ArgumentException("collectionName parameter is mandatory");
            if (document == null)
                throw new ArgumentException("document parameter is mandatory");

            var transformed = scope.ToBlittable(document.AsObject());

            if (collectionName.Equals(_configuration.Collection, StringComparison.OrdinalIgnoreCase) == false)
            {
                DynamicJsonValue metadata;

                if (transformed[Constants.Documents.Metadata.Key] != null)
                {
                    metadata = transformed[Constants.Documents.Metadata.Key] as DynamicJsonValue;
                }
                else
                {
                    transformed[Constants.Documents.Metadata.Key] = metadata = new DynamicJsonValue();
                }

                metadata[Constants.Documents.Metadata.Collection] = collectionName;
                metadata[Constants.Documents.Metadata.Id] = $"{_currentlyTransformed.DocumentKey}/{collectionName.ToLowerInvariant()}/";
            }

            var transformResult = Context.ReadObject(transformed, _currentlyTransformed.DocumentKey);

            _commands.Add(new PutCommandDataWithBlittableJson(_currentlyTransformed.DocumentKey, null, transformResult));
        }

        public override IEnumerable<ICommandData> GetTransformedResults()
        {
            return _commands;
        }

        public override void Transform(RavenEtlItem item)
        {
            if (item.IsDelete)
                _commands.Add(new DeleteCommandData(item.DocumentKey, null));
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