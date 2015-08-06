// -----------------------------------------------------------------------
//  <copyright file="TransformerActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Runtime.CompilerServices;

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
                    indexName =>
                    {
	                    var definitions = IndexDefinitionStorage.GetTransformerDefinition(indexName);
	                    return new RavenJObject
	                                 {
		                                 {"name", new RavenJValue(indexName) },
		                                 {"definition", RavenJObject.FromObject(definitions)},
										 {"lockMode",new RavenJValue(definitions.LockMode.ToString())}
	                                 };
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
	        if (existingDefinition != null)
	        {
		        switch (existingDefinition.LockMode)
		        {
			        case TransformerLockMode.Unlock:
				        if (existingDefinition.Equals(definition))
					        return name; // no op for the same transformer
				        break;
			        case TransformerLockMode.LockedIgnore:
						Log.Info("Transformer {0} not saved because it was lock (with ignore)", name);
                        return name;
			        default:
				        throw new ArgumentOutOfRangeException();
		        }
	        }

			var generator = IndexDefinitionStorage.CompileTransform(definition);

			if (existingDefinition != null)
				IndexDefinitionStorage.RemoveTransformer(existingDefinition.TransfomerId);

            TransactionalStorage.Batch(accessor =>
            {
                definition.TransfomerId = (int)Database.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("TransformerId", accessor);
            });

			IndexDefinitionStorage.CreateAndPersistTransform(definition, generator);
            IndexDefinitionStorage.AddTransform(definition.TransfomerId, definition);

            TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => Database.Notifications.RaiseNotifications(new TransformerChangeNotification()
            {
                Name = name,
                Type = TransformerChangeTypes.TransformerAdded,
            }));

            return name;
        }
    }
}