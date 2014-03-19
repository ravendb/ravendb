// -----------------------------------------------------------------------
//  <copyright file="TransformerActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Runtime.CompilerServices;

using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class TransformerActions : ActionsBase
    {
        public TransformerActions(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
            : base(database, recentTouches, uuidGenerator, log)
        {
        }

        public RavenJArray GetTransformerNames(int start, int pageSize)
        {
            return new RavenJArray(
            IndexDefinitionStorage.TransformerNames.Skip(start).Take(pageSize)
                .Select(s => new RavenJValue(s))
            );
        }

        public RavenJArray GetTransformers(int start, int pageSize)
        {
            return new RavenJArray(
            IndexDefinitionStorage.TransformerNames.Skip(start).Take(pageSize)
                .Select(
                    indexName => new RavenJObject
							{
								{"name", new RavenJValue(indexName) },
								{"definition", RavenJObject.FromObject(IndexDefinitionStorage.GetTransformerDefinition(indexName))}
							}));

        }

        public TransformerDefinition GetTransformerDefinition(string name)
        {
            return IndexDefinitionStorage.GetTransformerDefinition(name);
        }

        public void DeleteTransform(string name)
        {
            IndexDefinitionStorage.RemoveTransformer(name);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public string PutTransform(string name, TransformerDefinition definition)
        {
            if (name == null) throw new ArgumentNullException("name");
            if (definition == null) throw new ArgumentNullException("definition");

            name = name.Trim();

            var existingDefinition = IndexDefinitionStorage.GetTransformerDefinition(name);
            if (existingDefinition != null && existingDefinition.Equals(definition))
                return name; // no op for the same transformer

            TransactionalStorage.Batch(accessor =>
            {
                definition.TransfomerId = (int)Database.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("TransformerId", accessor, null);
            });

            IndexDefinitionStorage.CreateAndPersistTransform(definition);
            IndexDefinitionStorage.AddTransform(definition.TransfomerId, definition);

            return name;
        }
    }
}