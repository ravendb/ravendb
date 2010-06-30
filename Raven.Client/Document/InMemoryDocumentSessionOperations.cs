using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Client.Exceptions;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Json;

#if !NET_3_5
using System.Dynamic;
using Microsoft.CSharp.RuntimeBinder;
using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

#endif

namespace Raven.Client.Document
{
	public abstract class InMemoryDocumentSessionOperations : IInMemoryDocumentSessionOperations
	{
		public static readonly Guid RavenDbResourceManagerId = new Guid("E749BAA6-6F76-4EEF-A069-40A4378954F8");

		private const string RavenEntityName = "Raven-Entity-Name";
		protected readonly HashSet<object> deletedEntities = new HashSet<object>();

		private bool hasEnlisted;

		protected readonly Dictionary<object, DocumentSession.DocumentMetadata> entitiesAndMetadata =
			new Dictionary<object, DocumentSession.DocumentMetadata>();

		protected readonly Dictionary<string, object> entitiesByKey = new Dictionary<string, object>();
		protected DocumentStore documentStore;
		private int numberOfRequests;

		protected InMemoryDocumentSessionOperations(DocumentStore documentStore)
		{
			this.documentStore = documentStore;
		    UseOptimisticConcurrency = false;
			AllowNonAuthoritiveInformation = true;
		    MaxNumberOfRequestsPerSession = documentStore.Conventions.MaxNumberOfRequestsPerSession;
		}


		public string StoreIdentifier
		{
			get { return documentStore.Identifier; }
		}

		public DocumentConvention Conventions
		{
			get { return documentStore.Conventions; }
		}

		public int MaxNumberOfRequestsPerSession { get; set; }

		public bool UseOptimisticConcurrency { get; set; }
		public virtual event EntityStored Stored;
		public virtual event EntityToDocument OnEntityConverted;

		public JObject GetMetadataFor<T>(T instance)
		{
			DocumentSession.DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(instance, out value) == false)
				return null;
			return value.Metadata;
		}

		public bool HasChanges
		{
			get 
			{
				return deletedEntities.Count > 0 ||
						entitiesAndMetadata.Where(pair => EntityChanged(pair.Key, pair.Value)).Any();
			}
		}


		public bool HasChanged(object entity)
		{
			DocumentSession.DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
				return false;
			return EntityChanged(entity, value);
		}

		internal void IncrementRequestCount()
		{
			if (++numberOfRequests > MaxNumberOfRequestsPerSession)
				throw new InvalidOperationException(
					string.Format(
						@"The maximum number of requests ({0}) allowed for this session has been reached.
Raven limits the number of remote calls that a session is allowed to make as an early warning system. Sessions are expected to be short lived, and 
Raven provides facilities like Load(string[] keys) to load multiple documents at once and batch saves.
You can increase the limit by setting DocumentConvention.MaxNumberOfRequestsPerSession or DocumentSession.MaxNumberOfRequestsPerSession, but it is
advisable that you'll look into reducing the number of remote calls first, since that will speed up your application signficantly and result in a 
more responsive application.
",
						MaxNumberOfRequestsPerSession));
		}

		protected T TrackEntity<T>(JsonDocument documentFound)
		{
			if (documentFound.Metadata.Property("@etag") == null)
			{
				documentFound.Metadata.Add("@etag", new JValue(documentFound.Etag.ToString()));
			}
			if(documentFound.NonAuthoritiveInformation && 
				AllowNonAuthoritiveInformation == false)
			{
				throw new NonAuthoritiveInformationException("Document " + documentFound.Key +
				" returned Non Authoritive Information (probably modified by a transaction in progress) and AllowNonAuthoritiveInformation  is set to false");
			}
			return TrackEntity<T>(documentFound.Key, documentFound.DataAsJson, documentFound.Metadata);
		}

		public T TrackEntity<T>(string key, JObject document, JObject metadata)
		{
			object entity;
			if (entitiesByKey.TryGetValue(key, out entity) == false)
			{
				entity = ConvertToEntity<T>(key, document, metadata);
			}
			else
			{
				// the local instnace may have been changed, we adhere to the current Unit of Work
				// instance, and return that, ignoring anything new.
				return (T) entity;
			}
			var etag = metadata.Value<string>("@etag");
			if(metadata.Value<bool>("Non-Authoritive-Information") && 
				AllowNonAuthoritiveInformation == false)
			{
				throw new NonAuthoritiveInformationException("Document " + key +
					" returned Non Authoritive Information (probably modified by a transaction in progress) and AllowNonAuthoritiveInformation  is set to false");
			}
			entitiesAndMetadata[entity] = new DocumentSession.DocumentMetadata
			{
				OriginalValue = document,
				Metadata = metadata,
				OriginalMetadata = metadata,
				ETag = new Guid(etag),
				Key = key
			};
			entitiesByKey[key] = entity;
			return (T) entity;
		}

		public bool AllowNonAuthoritiveInformation { get; set; }

		public void Delete<T>(T entity)
		{
			deletedEntities.Add(entity);
		}

		protected object ConvertToEntity<T>(string id, JObject documentFound, JObject metadata)
		{
			var entity = default(T);

			var documentType = metadata.Value<string>("Raven-Clr-Type");
			if (documentType != null)
			{
				var type = Type.GetType(documentType);
				if (type != null)
					entity = (T) documentFound.Deserialize(type, Conventions.JsonContractResolver);
			}
			if (Equals(entity, default(T)))
			{
				entity = documentFound.Deserialize<T>(Conventions.JsonContractResolver);
			}
			var identityProperty = documentStore.Conventions.GetIdentityProperty(entity.GetType());
			if (identityProperty != null)
				identityProperty.SetValue(entity, id, null);
			return entity;
		}

		public void Store(object entity)
		{
			if (null == entity)
				throw new ArgumentNullException("entity");
			
			string id = null;
#if !NET_3_5
            if (entity is IDynamicMetaObjectProvider)
            {
            	if(TryGetId(entity,out id) == false)
				{
					id = Conventions.DocumentKeyGenerator(entity);

					if (id != null)
					{
						// Store it back into the Id field so the client has access to to it                    
						((dynamic) entity).Id = id;
					}
				}
            }
            else
#endif
            {
                var identityProperty = GetIdentityProperty(entity.GetType());
                if (identityProperty != null)
                    id = identityProperty.GetValue(entity, null) as string;

                if (id == null)
                {
                    // Generate the key up front
                    id = Conventions.GenerateDocumentKey(entity);

                    if (id != null && identityProperty != null)
                    {
                        // And store it so the client has access to to it
                        identityProperty.SetValue(entity, id, null);
                    }
                }
            }

			// we make the check here even if we just generated the key
			// users can override the key generation behavior, and we need
			// to detect if they generate duplicates.
			if (id != null &&
				id.EndsWith("/") == false // not a prefix id
					&& entitiesByKey.ContainsKey(id))
			{
				if (ReferenceEquals(entitiesByKey[id], entity))
					return; // calling Store twice on the same reference is a no-op
				throw new NonUniqueObjectException("Attempted to associated a different object with id '" + id + "'.");
			}

			var tag = documentStore.Conventions.GetTypeTagName(entity.GetType());
			entitiesAndMetadata.Add(entity, new DocumentSession.DocumentMetadata
			{
				Key = id,
				Metadata = new JObject(new JProperty(RavenEntityName, new JValue(tag))),
				OriginalMetadata = new JObject(),
				ETag = null,
				OriginalValue = new JObject()
			});
			if (id != null)
				entitiesByKey[id] = entity;
		}

#if !NET_3_5
		private static bool TryGetId(dynamic entity, out string id)
		{
			try
			{
				id = entity.Id;
				return true;
			}
			catch (RuntimeBinderException)
			{
				id = null;
				return false;
			}
		}
#endif

		protected ICommandData CreatePutEntityCommand(object entity, DocumentSession.DocumentMetadata documentMetadata)
		{
			var json = ConvertEntityToJson(entity, documentMetadata.Metadata);
			var entityType = entity.GetType();
			

            //This fails to find the key if it's a dynamic object

            string key = null;
#if !NET_3_5			
            if (entity is IDynamicMetaObjectProvider)
            {
            	TryGetId(entity,out key);
            }
            else
#endif
            {
                var identityProperty = GetIdentityProperty(entityType);
                if (identityProperty != null)
                    key = (string)identityProperty.GetValue(entity, null);
            }
			var etag = UseOptimisticConcurrency ? documentMetadata.ETag : null;

			return new PutCommandData
			{
				Document = json,
				Etag = etag,
				Key = key,
				Metadata = documentMetadata.Metadata,
			};
		}

		private PropertyInfo GetIdentityProperty(Type entityType)
		{
			return documentStore.Conventions.GetIdentityProperty(entityType);
		}

		protected void UpdateBatchResults(IList<BatchResult> batchResults, IList<object> entities)
		{
			var stored = Stored;
			for (var i = 0; i < batchResults.Count; i++)
			{
				var batchResult = batchResults[i];
				if (batchResult.Method != "PUT")
					continue;

				var entity = entities[i];
				DocumentSession.DocumentMetadata documentMetadata;
				if (entitiesAndMetadata.TryGetValue(entity, out documentMetadata) == false)
					continue;

				entitiesByKey[batchResult.Key] = entity;
				documentMetadata.ETag = batchResult.Etag;
				documentMetadata.Key = batchResult.Key;
				documentMetadata.OriginalMetadata = batchResult.Metadata;
				documentMetadata.OriginalValue = ConvertEntityToJson(entity, documentMetadata.Metadata);

				// Set/Update the id of the entity
				var identityProperty = GetIdentityProperty(entity.GetType());
				if (identityProperty != null && identityProperty.CanWrite)
					// this is allowed because we need to support store anonymous types
					identityProperty.SetValue(entity, batchResult.Key, null);

				if (stored != null)
					stored(entity);
            }
		}

		protected DocumentSession.SaveChangesData PrepareForSaveChanges()
		{
			var result = new DocumentSession.SaveChangesData
			{
				Entities = new List<object>(),
				Commands = new List<ICommandData>()
			};
			TryEnlistInAmbientTransaction();
			DocumentSession.DocumentMetadata value = null;
			foreach (var key in (from deletedEntity in deletedEntities
								 where entitiesAndMetadata.TryGetValue(deletedEntity, out value)
								 select value.Key))
			{
				Guid? etag = null;
				object existingEntity;
				if (entitiesByKey.TryGetValue(key, out existingEntity))
				{
					DocumentSession.DocumentMetadata metadata;
					if (entitiesAndMetadata.TryGetValue(existingEntity, out metadata))
						etag = metadata.ETag;
					entitiesAndMetadata.Remove(existingEntity);
					entitiesByKey.Remove(key);
				}

				etag = UseOptimisticConcurrency ? etag : null;
				result.Entities.Add(existingEntity);
				result.Commands.Add(new DeleteCommandData
				{
					Etag = etag,
					Key = key,
				});
			}
			deletedEntities.Clear();
			foreach (var entity in entitiesAndMetadata.Where(pair => EntityChanged(pair.Key, pair.Value)))
			{
				result.Entities.Add(entity.Key);
				if (entity.Value.Key != null)
					entitiesByKey.Remove(entity.Value.Key);
				result.Commands.Add(CreatePutEntityCommand(entity.Key, entity.Value));
			}

			return result;
		}

		private void TryEnlistInAmbientTransaction()
		{
			if (hasEnlisted || Transaction.Current == null) 
				return;


			var transactionalSession = (ITransactionalDocumentSession)this;
			if (documentStore.DatabaseCommands.SupportsPromotableTransactions == false ||
				Transaction.Current.EnlistPromotableSinglePhase(new PromotableRavenClientEnlistment(transactionalSession)) == false) 
			{
				Transaction.Current.EnlistDurable(
					RavenDbResourceManagerId, 
					new RavenClientEnlistment(transactionalSession),
					EnlistmentOptions.None);
			}
			hasEnlisted = true;
		}

		protected bool EntityChanged(object entity, DocumentSession.DocumentMetadata documentMetadata)
		{
			if (documentMetadata == null)
				return true; 
			var newObj = ConvertEntityToJson(entity, documentMetadata.Metadata);
			var equalityComparer = new JTokenEqualityComparer();
			return equalityComparer.Equals(newObj, documentMetadata.OriginalValue) == false ||
				equalityComparer.Equals(documentMetadata.Metadata, documentMetadata.OriginalMetadata) == false;
		}

		private JObject ConvertEntityToJson(object entity, JObject metadata)
		{
			var entityType = entity.GetType();
			var identityProperty = documentStore.Conventions.GetIdentityProperty(entityType);

			var objectAsJson = JObject.FromObject(entity, new JsonSerializer
			{
				Converters = {new JsonEnumConverter()},
				ContractResolver = Conventions.JsonContractResolver,
				ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor
			});
			if (identityProperty != null)
			{
				objectAsJson.Remove(identityProperty.Name);
			}

			metadata["Raven-Clr-Type"] = JToken.FromObject(entityType.FullName + ", " + entityType.Assembly.GetName().Name);

			var entityConverted = OnEntityConverted;
			if (entityConverted != null)
				entityConverted(entity, objectAsJson, metadata);

			return objectAsJson;
		}

		public void Evict<T>(T entity)
		{
			DocumentSession.DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value))
			{
				entitiesAndMetadata.Remove(entity);
				entitiesByKey.Remove(value.Key);
			}
			deletedEntities.Remove(entity);
		}

		public void Clear()
		{
			entitiesAndMetadata.Clear();
			deletedEntities.Clear();
			entitiesByKey.Clear();
		}

		public virtual void Dispose()
		{
			
		}

		public abstract void Commit(Guid txId);
		public abstract void Rollback(Guid txId);
		public abstract byte[] PromoteTransaction(Guid fromTxId);

		protected void ClearEnlistment()
		{
			hasEnlisted = false;
		}
	}
}