// -----------------------------------------------------------------------
//  <copyright file="TransformerActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Abstractions.Data;
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
		/// <summary>
		/// For temporary transformers we assign negative indexes
		/// </summary>
		private int temporaryTransfomerIndex = -1;

        public TransformerActions(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
            : base(database, recentTouches, uuidGenerator, log)
        {
        }

	    internal string[] Names
	    {
		    get { return IndexDefinitionStorage.TransformerNames; }
	    }

	    internal TransformerDefinition[] Definitions
	    {
		    get { return IndexDefinitionStorage.GetAllTransformerDefinitions().ToArray(); }
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

        public bool DeleteTransform(string name)
        {
	        if (!IndexDefinitionStorage.RemoveTransformer(name)) 
				return false;

			//raise notification only if the transformer was actually removed
	        TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => Database.Notifications.RaiseNotifications(new TransformerChangeNotification
	        {
		        Name = name,
		        Type = TransformerChangeTypes.TransformerRemoved
	        }));

	        return true;
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

			var generator = IndexDefinitionStorage.CompileTransform(definition);

			if (existingDefinition != null)
				IndexDefinitionStorage.RemoveTransformer(existingDefinition.TransfomerId);

	        var temporary = definition.Temporary;

	        if (temporary)
	        {
		        definition.TransfomerId = Database.Transformers.GetNextTemporaryTransformerIndex();
				IndexDefinitionStorage.CreateTransform(definition, generator);
				IndexDefinitionStorage.AddTransform(definition.TransfomerId, definition);
	        }
	        else
	        {
				TransactionalStorage.Batch(accessor =>
				{
					definition.TransfomerId = (int)Database.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("TransformerId", accessor);
				});

				IndexDefinitionStorage.CreateTransform(definition, generator);
				IndexDefinitionStorage.PersistTransform(definition);
				IndexDefinitionStorage.AddTransform(definition.TransfomerId, definition);

				TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => Database.Notifications.RaiseNotifications(new TransformerChangeNotification()
				{
					Name = name,
					Type = TransformerChangeTypes.TransformerAdded,
				}));
	        }

            return name;
        }

		public int GetNextTemporaryTransformerIndex()
		{
			return Interlocked.Decrement(ref temporaryTransfomerIndex);
		}
    }
}