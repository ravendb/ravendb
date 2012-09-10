//-----------------------------------------------------------------------
// <copyright file="InMemoryDocumentSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
#if !SILVERLIGHT
using System.Text.RegularExpressions;
using System.Transactions;
#endif
using System.Text;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Linq;
using Raven.Client.Connection;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Json.Linq;

#if !NET35
using System.Dynamic;
using Microsoft.CSharp.RuntimeBinder;
using System.Threading.Tasks;
using Raven.Abstractions.Json;

#endif

namespace Raven.Client.Document
{

	/// <summary>
	/// Abstract implementation for in memory session operations
	/// </summary>
	public abstract class InMemoryDocumentSessionOperations : IDisposable
	{
		protected bool GenerateDocumentKeysOnStore = true;

		/// <summary>
		/// The session id 
		/// </summary>
		public Guid Id { get; private set; }

		protected static readonly ILog log = LogProvider.GetCurrentClassLogger();

		/// <summary>
		/// The entities waiting to be deleted
		/// </summary>
		protected readonly HashSet<object> deletedEntities = new HashSet<object>(ObjectReferenceEqualityComparerer<object>.Default);

		/// <summary>
		/// Entities whose id we already know do not exists, because they are a missing include, or a missing load, etc.
		/// </summary>
		protected readonly HashSet<string> knownMissingIds = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);

#if !SILVERLIGHT
		private bool hasEnlisted;
		[ThreadStatic]
		private static Dictionary<string, HashSet<string>> _registeredStoresInTransaction;

		private static Dictionary<string, HashSet<string>> RegisteredStoresInTransaction
		{
			get { return (_registeredStoresInTransaction ?? (_registeredStoresInTransaction = new Dictionary<string, HashSet<string>>())); }
		}
#endif

		/// <summary>
		/// hold the data required to manage the data for RavenDB's Unit of Work
		/// </summary>
		protected readonly Dictionary<object, DocumentMetadata> entitiesAndMetadata =
			new Dictionary<object, DocumentMetadata>(ObjectReferenceEqualityComparerer<object>.Default);

		/// <summary>
		/// Translate between a key and its associated entity
		/// </summary>
		protected readonly Dictionary<string, object> entitiesByKey = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);

		private readonly DocumentStoreBase documentStore;

		/// <summary>
		/// all the listeners for this session
		/// </summary>
		protected readonly DocumentSessionListeners listeners;

		///<summary>
		/// The document store associated with this session
		///</summary>
		public IDocumentStore DocumentStore
		{
			get { return documentStore; }
		}


		/// <summary>
		/// Gets the number of requests for this session
		/// </summary>
		/// <value></value>
		public int NumberOfRequests { get; private set; }

		private IDictionary<object, RavenJObject> cachedJsonDocs;

		/// <summary>
		/// Initializes a new instance of the <see cref="InMemoryDocumentSessionOperations"/> class.
		/// </summary>
		protected InMemoryDocumentSessionOperations(
			DocumentStoreBase documentStore,
			DocumentSessionListeners listeners,
			Guid id)
		{
			Id = id;
			this.documentStore = documentStore;
			this.listeners = listeners;
			ResourceManagerId = documentStore.ResourceManagerId;
			UseOptimisticConcurrency = false;
			AllowNonAuthoritativeInformation = true;
			NonAuthoritativeInformationTimeout = TimeSpan.FromSeconds(15);
			MaxNumberOfRequestsPerSession = documentStore.Conventions.MaxNumberOfRequestsPerSession;
		}

		/// <summary>
		/// Gets or sets the timeout to wait for authoritative information if encountered non authoritative document.
		/// </summary>
		/// <value></value>
		public TimeSpan NonAuthoritativeInformationTimeout { get; set; }

		/// <summary>
		/// Gets the store identifier for this session.
		/// The store identifier is the identifier for the particular RavenDB instance.
		/// </summary>
		/// <value>The store identifier.</value>
		public string StoreIdentifier
		{
			get { return documentStore.Identifier; }
		}

		/// <summary>
		/// Gets the conventions used by this session
		/// </summary>
		/// <value>The conventions.</value>
		/// <remarks>
		/// This instance is shared among all sessions, changes to the <see cref="DocumentConvention"/> should be done
		/// via the <see cref="IDocumentStore"/> instance, not on a single session.
		/// </remarks>
		public DocumentConvention Conventions
		{
			get { return documentStore.Conventions; }
		}

		/// <summary>
		/// The transaction resource manager identifier
		/// </summary>
		public Guid ResourceManagerId { get; private set; }


		/// <summary>
		/// Gets or sets the max number of requests per session.
		/// If the <see cref="NumberOfRequests"/> rise above <see cref="MaxNumberOfRequestsPerSession"/>, an exception will be thrown.
		/// </summary>
		/// <value>The max number of requests per session.</value>
		public int MaxNumberOfRequestsPerSession { get; set; }

		/// <summary>
		/// Gets or sets a value indicating whether the session should use optimistic concurrency.
		/// When set to <c>true</c>, a check is made so that a change made behind the session back would fail
		/// and raise <see cref="ConcurrencyException"/>.
		/// </summary>
		/// <value></value>
		public bool UseOptimisticConcurrency { get; set; }

		/// <summary>
		/// Gets the ETag for the specified entity.
		/// If the entity is transient, it will load the etag from the store
		/// and associate the current state of the entity with the etag from the server.
		/// </summary>
		/// <param name="instance">The instance.</param>
		/// <returns></returns>
		public Guid? GetEtagFor<T>(T instance)
		{
			return GetDocumentMetadata(instance).ETag;
		}

		/// <summary>
		/// Gets the metadata for the specified entity.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="instance">The instance.</param>
		/// <returns></returns>
		public RavenJObject GetMetadataFor<T>(T instance)
		{
			return GetDocumentMetadata(instance).Metadata;
		}

		private DocumentMetadata GetDocumentMetadata<T>(T instance)
		{
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(instance, out value) == false)
			{
				string id;
				if (TryGetIdFromInstance(instance, out id)
#if !NET35
					|| (instance is IDynamicMetaObjectProvider &&
					   TryGetIdFromDynamic(instance, out id))
#endif
)
				{
					AssertNoNonUniqueInstance(instance, id);

					var jsonDocument = GetJsonDocument(id);
					entitiesByKey[id] = instance;
					entitiesAndMetadata[instance] = value = new DocumentMetadata
					{
						ETag = UseOptimisticConcurrency ? (Guid?)Guid.Empty : null,
						Key = id,
						OriginalMetadata = jsonDocument.Metadata,
						Metadata = (RavenJObject)jsonDocument.Metadata.CloneToken(),
						OriginalValue = new RavenJObject()
					};
				}
				else
				{
					throw new InvalidOperationException("Could not find the document key for " + instance);
				}
			}
			return value;
		}

		/// <summary>
		/// Get the json document by key from the store
		/// </summary>
		protected abstract JsonDocument GetJsonDocument(string documentKey);

		/// <summary>
		/// Returns whatever a document with the specified id is loaded in the 
		/// current session
		/// </summary>
		public bool IsLoaded(string id)
		{
			return entitiesByKey.ContainsKey(id);
		}

		/// <summary>
		/// Returns whatever a document with the specified id is deleted 
		/// or known to be missing
		/// </summary>
		public bool IsDeleted(string id)
		{
			return knownMissingIds.Contains(id);
		}


		/// <summary>
		/// Gets the document id.
		/// </summary>
		/// <param name="instance">The instance.</param>
		/// <returns></returns>
		public string GetDocumentId(object instance)
		{
			if (instance == null)
				return null;
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(instance, out value) == false)
				return null;
			return value.Key;
		}
		/// <summary>
		/// Gets a value indicating whether any of the entities tracked by the session has changes.
		/// </summary>
		/// <value></value>
		public bool HasChanges
		{
			get
			{
				return deletedEntities.Count > 0 ||
						entitiesAndMetadata.Any(pair => EntityChanged(pair.Key, pair.Value));
			}
		}



		/// <summary>
		/// Determines whether the specified entity has changed.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns>
		/// 	<c>true</c> if the specified entity has changed; otherwise, <c>false</c>.
		/// </returns>
		public bool HasChanged(object entity)
		{
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
				return false;
			return EntityChanged(entity, value);
		}

		public void IncrementRequestCount()
		{
			if (++NumberOfRequests > MaxNumberOfRequestsPerSession)
				throw new InvalidOperationException(
					string.Format(
						@"The maximum number of requests ({0}) allowed for this session has been reached.
Raven limits the number of remote calls that a session is allowed to make as an early warning system. Sessions are expected to be short lived, and 
Raven provides facilities like Load(string[] keys) to load multiple documents at once and batch saves (call SaveChanges() only once).
You can increase the limit by setting DocumentConvention.MaxNumberOfRequestsPerSession or MaxNumberOfRequestsPerSession, but it is
advisable that you'll look into reducing the number of remote calls first, since that will speed up your application significantly and result in a 
more responsive application.
",
						MaxNumberOfRequestsPerSession));
		}

		/// <summary>
		/// Tracks the entity inside the unit of work
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="documentFound">The document found.</param>
		/// <returns></returns>
		public T TrackEntity<T>(JsonDocument documentFound)
		{
			if (documentFound.NonAuthoritativeInformation.HasValue
				&& documentFound.NonAuthoritativeInformation.Value
				&& AllowNonAuthoritativeInformation == false)
			{
				throw new NonAuthoritativeInformationException("Document " + documentFound.Key +
				" returned Non Authoritative Information (probably modified by a transaction in progress) and AllowNonAuthoritativeInformation  is set to false");
			}
			if (documentFound.Metadata.Value<bool?>(Constants.RavenDocumentDoesNotExists) == true)
			{
				return default(T); // document is not really there.
			}
			if (!documentFound.Metadata.ContainsKey("@etag"))
			{
				documentFound.Metadata["@etag"] = documentFound.Etag.ToString();
			}
			if (!documentFound.Metadata.ContainsKey(Constants.LastModified))
			{
				documentFound.Metadata[Constants.LastModified] = documentFound.LastModified;
			}

			return TrackEntity<T>(documentFound.Key, documentFound.DataAsJson, documentFound.Metadata);
		}

		/// <summary>
		/// Tracks the entity.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="key">The key.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		/// <returns></returns>
		public T TrackEntity<T>(string key, RavenJObject document, RavenJObject metadata)
		{
			document.Remove("@metadata");

			object entity;
			if (entitiesByKey.TryGetValue(key, out entity) == false)
			{
				entity = ConvertToEntity<T>(key, document, metadata);
			}
			else
			{
				// the local instance may have been changed, we adhere to the current Unit of Work
				// instance, and return that, ignoring anything new.
				return (T)entity;
			}
			var etag = metadata.Value<string>("@etag");
			if (metadata.Value<bool>("Non-Authoritative-Information") &&
				AllowNonAuthoritativeInformation == false)
			{
				throw new NonAuthoritativeInformationException("Document " + key +
					" returned Non Authoritative Information (probably modified by a transaction in progress) and AllowNonAuthoritativeInformation  is set to false");
			}
			entitiesAndMetadata[entity] = new DocumentMetadata
			{
				OriginalValue = document,
				Metadata = metadata,
				OriginalMetadata = (RavenJObject)metadata.CloneToken(),
				ETag = HttpExtensions.EtagHeaderToGuid(etag),
				Key = key
			};
			entitiesByKey[key] = entity;
			return (T)entity;
		}

		/// <summary>
		/// Gets or sets a value indicating whether non authoritative information is allowed.
		/// Non authoritative information is document that has been modified by a transaction that hasn't been committed.
		/// The server provides the latest committed version, but it is known that attempting to write to a non authoritative document
		/// will fail, because it is already modified.
		/// If set to <c>false</c>, the session will wait <see cref="NonAuthoritativeInformationTimeout"/> for the transaction to commit to get an
		/// authoritative information. If the wait is longer than <see cref="NonAuthoritativeInformationTimeout"/>, <see cref="NonAuthoritativeInformationException"/> is thrown.
		/// </summary>
		/// <value>
		/// 	<c>true</c> if non authoritative information is allowed; otherwise, <c>false</c>.
		/// </value>
		public bool AllowNonAuthoritativeInformation { get; set; }

		/// <summary>
		/// Marks the specified entity for deletion. The entity will be deleted when SaveChanges is called.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entity">The entity.</param>
		public void Delete<T>(T entity)
		{
			if(ReferenceEquals(entity,null)) throw new ArgumentNullException("entity");
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
				throw new InvalidOperationException(entity + " is not associated with the session, cannot delete unknown entity instance");
			if (value.OriginalMetadata.ContainsKey(Constants.RavenReadOnly) && value.OriginalMetadata.Value<bool>(Constants.RavenReadOnly))
				throw new InvalidOperationException(entity + " is marked as read only and cannot be deleted");
			deletedEntities.Add(entity);
			knownMissingIds.Add(value.Key);
		}


		/// <summary>
		/// Converts the json document to an entity.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="id">The id.</param>
		/// <param name="documentFound">The document found.</param>
		/// <param name="metadata">The metadata.</param>
		/// <returns></returns>
		protected object ConvertToEntity<T>(string id, RavenJObject documentFound, RavenJObject metadata)
		{
			if (typeof(T) == typeof(RavenJObject))
				return (T)(object)documentFound.CloneToken();

			var entity = default(T);
			EnsureNotReadVetoed(metadata);
			var documentType = Conventions.GetClrType(id, documentFound, metadata);
			if (documentType != null)
			{
				var type = Type.GetType(documentType);
				if (type != null)
					entity = (T)documentFound.Deserialize(type, Conventions);
			}
			if (Equals(entity, default(T)))
			{
				entity = documentFound.Deserialize<T>(Conventions);
#if !NET35
				var document = entity as RavenJObject;
				if (document != null)
				{
					entity = (T)(object)(new DynamicJsonObject(document));
				}
#endif
			}
			TrySetIdentity(entity, id);

			foreach (var documentConversionListener in listeners.ConversionListeners)
			{
				documentConversionListener.DocumentToEntity(entity, documentFound, metadata);
			}

			return entity;
		}

		/// <summary>
		/// Tries to set the identity property
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entity">The entity.</param>
		/// <param name="id">The id.</param>
		protected internal void TrySetIdentity<T>(T entity, string id)
		{
			var entityType = entity.GetType();
			var identityProperty = documentStore.Conventions.GetIdentityProperty(entityType);
			if (identityProperty == null)
			{
#if !NET35
				if (entity is IDynamicMetaObjectProvider)
				{
					TrySetIdOnynamic(entity, id);
				}
#endif
				return;
			}

			if (identityProperty.CanWrite)
			{
				SetPropertyOrField(identityProperty.PropertyType, entity, val => identityProperty.SetValue(entity, val, null), id);
			}
			else
			{
				const BindingFlags privateInstanceField = BindingFlags.Instance | BindingFlags.NonPublic;
				var fieldInfo = entityType.GetField("<" + identityProperty.Name + ">i__Field", privateInstanceField) ??
								entityType.GetField("<" + identityProperty.Name + ">k__BackingField", privateInstanceField);

				if (fieldInfo == null)
					return;

				SetPropertyOrField(identityProperty.PropertyType, entity, val => fieldInfo.SetValue(entity, val), id);
			}
		}

		private void SetPropertyOrField(Type propertyOrFieldType, object entity, Action<object> setIdenitifer, string id)
		{
			if (propertyOrFieldType == typeof(string))
			{
				setIdenitifer(id);
			}
			else // need converting
			{
				var converter =
					Conventions.IdentityTypeConvertors.FirstOrDefault(x => x.CanConvertFrom(propertyOrFieldType));
				if (converter == null)
					throw new ArgumentException("Could not convert identity to type " + propertyOrFieldType +
												" because there is not matching type converter registered in the conventions' IdentityTypeConvertors");

				setIdenitifer(converter.ConvertTo(Conventions.FindIdValuePartForValueTypeConversion(entity, id)));
			}
		}

		private static void EnsureNotReadVetoed(RavenJObject metadata)
		{
			var readVeto = metadata["Raven-Read-Veto"] as RavenJObject;
			if (readVeto == null)
				return;

			var s = readVeto.Value<string>("Reason");
			throw new ReadVetoException(
				"Document could not be read because of a read veto." + Environment.NewLine +
				"The read was vetoed by: " + readVeto.Value<string>("Trigger") + Environment.NewLine +
				"Veto reason: " + s
				);
		}

		/// <summary>
		/// Stores the specified entity in the session. The entity will be saved when SaveChanges is called.
		/// </summary>
		public void Store(object entity)
		{
			string id;
			var hasId = TryGetIdFromInstance(entity, out id);
			StoreInternal(entity, null, null, forceConcurrencyCheck: hasId == false);
		}

		/// <summary>
		/// Stores the specified entity in the session. The entity will be saved when SaveChanges is called.
		/// </summary>
		public void Store(object entity, Guid etag)
		{
			StoreInternal(entity, etag, null, forceConcurrencyCheck: true);
		}

		/// <summary>
		/// Stores the specified entity in the session, explicitly specifying its Id. The entity will be saved when SaveChanges is called.
		/// </summary>
		public void Store(object entity, string id)
		{
			StoreInternal(entity, null, id, forceConcurrencyCheck: false);
		}

		/// <summary>
		/// Stores the specified entity in the session, explicitly specifying its Id. The entity will be saved when SaveChanges is called.
		/// </summary>
		public void Store(object entity, Guid etag, string id)
		{
			StoreInternal(entity, etag, id, forceConcurrencyCheck: true);
		}

		private void StoreInternal(object entity, Guid? etag, string id, bool forceConcurrencyCheck)
		{
			if (null == entity)
				throw new ArgumentNullException("entity");

			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value))
			{
				value.ForceConcurrencyCheck = forceConcurrencyCheck;
				return;
			}

			if (id == null)
			{
				if (GenerateDocumentKeysOnStore)
				{
					id = GenerateDocumentKeyForStorage(entity);
				}
				else
				{
					RememberEntityForDocumentKeyGeneration(entity);
				}
			}
			else
			{
				// Store it back into the Id field so the client has access to to it                    
				TrySetIdentity(entity, id);
			}

			// we make the check here even if we just generated the key
			// users can override the key generation behavior, and we need
			// to detect if they generate duplicates.
			AssertNoNonUniqueInstance(entity, id);

			var tag = documentStore.Conventions.GetTypeTagName(entity.GetType());
			var metadata = new RavenJObject();
			if (tag != null)
				metadata.Add(Constants.RavenEntityName, tag);
			if (id != null)
				knownMissingIds.Remove(id);
			StoreEntityInUnitOfWork(id, entity, etag, metadata, forceConcurrencyCheck);
		}

		protected string GenerateDocumentKeyForStorage(object entity)
		{
			string id;
#if !NET35
			if (entity is IDynamicMetaObjectProvider)
			{
				if (TryGetIdFromDynamic(entity, out id) == false)
				{
					id = GenerateKey(entity);
					// If we generated a new id, store it back into the Id field so the client has access to to it                    
					if (id != null)
						TrySetIdOnynamic(entity, id);
				}
				return id;
			}
#endif

			id = GetOrGenerateDocumentKey(entity);
			TrySetIdentity(entity, id);
			return id;
		}

		protected abstract string GenerateKey(object entity);

		protected virtual void RememberEntityForDocumentKeyGeneration(object entity)
		{
			throw new NotImplementedException("You cannot set GenerateDocumentKeysOnStore to false without implementing RememberEntityForDocumentKeyGeneration");
		}

#if !NET35
		protected internal Task<string> GenerateDocumentKeyForStorageAsync(object entity)
		{
			if (entity is IDynamicMetaObjectProvider)
			{
				string id;
				if (TryGetIdFromDynamic(entity, out id))
					return CompletedTask.With(id);
				else
					return GenerateKeyAsync(entity)
						.ContinueWith(task =>
						{
							// If we generated a new id, store it back into the Id field so the client has access to to it                    
							if (task.Result != null)
								TrySetIdOnynamic(entity, task.Result);
							return task.Result;
						});
			}
			
			return GetOrGenerateDocumentKeyAsync(entity)
				.ContinueWith(task =>
				{
					TrySetIdentity(entity, task.Result);
					return task.Result;
				});
		}

		protected abstract Task<string> GenerateKeyAsync(object entity);
#endif

		protected virtual void StoreEntityInUnitOfWork(string id, object entity, Guid? etag, RavenJObject metadata, bool forceConcurrencyCheck)
		{
			entitiesAndMetadata.Add(entity, new DocumentMetadata
			{
				Key = id,
				Metadata = metadata,
				OriginalMetadata = new RavenJObject(),
				ETag = etag,
				OriginalValue = new RavenJObject(),
				ForceConcurrencyCheck = forceConcurrencyCheck
			});
			if (id != null)
				entitiesByKey[id] = entity;
		}

		protected virtual void AssertNoNonUniqueInstance(object entity, string id)
		{
			if (id == null || id.EndsWith("/") || !entitiesByKey.ContainsKey(id) || ReferenceEquals(entitiesByKey[id], entity))
				return;

			throw new NonUniqueObjectException("Attempted to associate a different object with id '" + id + "'.");
		}

		/// <summary>
		/// Tries to get the identity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		protected string GetOrGenerateDocumentKey(object entity)
		{
			string id;
			TryGetIdFromInstance(entity, out id);

			if (id == null)
			{
				// Generate the key up front
				id = GenerateKey(entity);

			}

			if (id != null && id.StartsWith("/"))
				throw new InvalidOperationException("Cannot use value '" + id + "' as a document id because it begins with a '/'");
			return id;
		}

#if !NET35
		protected Task<string> GetOrGenerateDocumentKeyAsync(object entity)
		{
			string id;
			TryGetIdFromInstance(entity, out id);

			Task<string> generator =
				id != null
				? CompletedTask.With(id)
				: GenerateKeyAsync(entity);

			return generator.ContinueWith(task =>
			{
				if (task.Result != null && task.Result.StartsWith("/"))
					throw new InvalidOperationException("Cannot use value '" + id + "' as a document id because it begins with a '/'");

				return task.Result;
			});
		}
#endif

		/// <summary>
		/// Attempts to get the document key from an instance 
		/// </summary>
		protected bool TryGetIdFromInstance(object entity, out string id)
		{
			var identityProperty = GetIdentityProperty(entity.GetType());
			if (identityProperty != null)
			{
				var value = identityProperty.GetValue(entity, new object[0]);
				id = value as string;
				if (id == null && value != null) // need conversion
				{
					id = Conventions.FindFullDocumentKeyFromNonStringIdentifier(value, entity.GetType(), true);
					return true;
				}
				return id != null;
			}
			id = null;
			return false;
		}

#if !NET35
		private static bool TryGetIdFromDynamic(dynamic entity, out string id)
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

		private static void TrySetIdOnynamic(dynamic entity, string id)
		{
			try
			{
				entity.Id = id;
			}
			catch (RuntimeBinderException)
			{
			}
		}
#endif

		/// <summary>
		/// Creates the put entity command.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <param name="documentMetadata">The document metadata.</param>
		/// <returns></returns>
		protected ICommandData CreatePutEntityCommand(object entity, DocumentMetadata documentMetadata)
		{
			string id;
			if (TryGetIdFromInstance(entity, out id) &&
				documentMetadata.Key != null &&
				documentMetadata.Key.Equals(id, StringComparison.InvariantCultureIgnoreCase) == false)
			{
				throw new InvalidOperationException("Entity " + entity.GetType().FullName + " had document key '" +
													documentMetadata.Key + "' but now has document key property '" + id + "'." +
													Environment.NewLine +
													"You cannot change the document key property of a entity loaded into the session");
			}

			var json = ConvertEntityToJson(entity, documentMetadata.Metadata);

			var etag = UseOptimisticConcurrency || documentMetadata.ForceConcurrencyCheck
						   ? (documentMetadata.ETag ?? Guid.Empty)
						   : (Guid?)null;

			return new PutCommandData
			{
				Document = json,
				Etag = etag,
				Key = documentMetadata.Key,
				Metadata = documentMetadata.Metadata,
			};
		}

		private PropertyInfo GetIdentityProperty(Type entityType)
		{
			return documentStore.Conventions.GetIdentityProperty(entityType);
		}

		/// <summary>
		/// Updates the batch results.
		/// </summary>
		protected void UpdateBatchResults(IList<BatchResult> batchResults, SaveChangesData saveChangesData)
		{
			for (var i = saveChangesData.DeferredCommandsCount; i < batchResults.Count; i++)
			{
				var batchResult = batchResults[i];
				if (batchResult.Method != "PUT")
					continue;

				var entity = saveChangesData.Entities[i - saveChangesData.DeferredCommandsCount];
				DocumentMetadata documentMetadata;
				if (entitiesAndMetadata.TryGetValue(entity, out documentMetadata) == false)
					continue;

				batchResult.Metadata["@etag"] = new RavenJValue(batchResult.Etag.ToString());
				entitiesByKey[batchResult.Key] = entity;
				documentMetadata.ETag = batchResult.Etag;
				documentMetadata.Key = batchResult.Key;
				documentMetadata.OriginalMetadata = (RavenJObject)batchResult.Metadata.CloneToken();
				documentMetadata.Metadata = batchResult.Metadata;
				documentMetadata.OriginalValue = ConvertEntityToJson(entity, documentMetadata.Metadata);

				TrySetIdentity(entity, batchResult.Key);

				foreach (var documentStoreListener in listeners.StoreListeners)
				{
					documentStoreListener.AfterStore(batchResult.Key, entity, batchResult.Metadata);
				}
			}

			var lastPut = batchResults.LastOrDefault(x => x.Method == "PUT");
			if (lastPut == null)
				return;

			documentStore.UpdateLastWrittenEtag(lastPut.Etag);
		}

		/// <summary>
		/// Prepares for save changes.
		/// </summary>
		/// <returns></returns>
		protected SaveChangesData PrepareForSaveChanges()
		{
			cachedJsonDocs.Clear();
			var result = new SaveChangesData
			{
				Entities = new List<object>(),
				Commands = new List<ICommandData>(deferedCommands),
				DeferredCommandsCount = deferedCommands.Count
			};
			deferedCommands.Clear();

#if !SILVERLIGHT
			if (documentStore.EnlistInDistributedTransactions)
				TryEnlistInAmbientTransaction();
#endif
			PrepareForEntitiesDeletion(result);
			PrepareForEntitiesPuts(result);

			return result;
		}

		private void PrepareForEntitiesPuts(SaveChangesData result)
		{
			foreach (var entity in entitiesAndMetadata.Where(pair => EntityChanged(pair.Key, pair.Value)).ToArray())
			{
				foreach (var documentStoreListener in listeners.StoreListeners)
				{
					if (documentStoreListener.BeforeStore(entity.Value.Key, entity.Key, entity.Value.Metadata, entity.Value.OriginalValue))
						cachedJsonDocs.Remove(entity.Key);
				}
				result.Entities.Add(entity.Key);
				if (entity.Value.Key != null)
					entitiesByKey.Remove(entity.Value.Key);
				result.Commands.Add(CreatePutEntityCommand(entity.Key, entity.Value));
			}
		}

		private void PrepareForEntitiesDeletion(SaveChangesData result)
		{
			DocumentMetadata value = null;

			var keysToDelete = (from deletedEntity in deletedEntities
								where entitiesAndMetadata.TryGetValue(deletedEntity, out value)
								// skip deleting read only entities
								where !value.OriginalMetadata.ContainsKey(Constants.RavenReadOnly) ||
									  !value.OriginalMetadata.Value<bool>(Constants.RavenReadOnly)
								select value.Key).ToList();

			foreach (var key in keysToDelete)
			{
				Guid? etag = null;
				object existingEntity;
				DocumentMetadata metadata = null;
				if (entitiesByKey.TryGetValue(key, out existingEntity))
				{
					if (entitiesAndMetadata.TryGetValue(existingEntity, out metadata))
						etag = metadata.ETag;
					entitiesAndMetadata.Remove(existingEntity);
					entitiesByKey.Remove(key);
				}

				etag = UseOptimisticConcurrency ? etag : null;
				result.Entities.Add(existingEntity);

				foreach (var deleteListener in listeners.DeleteListeners)
				{
					deleteListener.BeforeDelete(key, existingEntity, metadata != null ? metadata.Metadata : null);
				}

				result.Commands.Add(new DeleteCommandData
				{
					Etag = etag,
					Key = key,
				});
			}
			deletedEntities.Clear();
		}

#if !SILVERLIGHT
		protected virtual void TryEnlistInAmbientTransaction()
		{

			if (hasEnlisted || Transaction.Current == null)
				return;

			HashSet<string> registered;
			var localIdentifier = Transaction.Current.TransactionInformation.LocalIdentifier;
			if (RegisteredStoresInTransaction.TryGetValue(localIdentifier, out registered) == false)
			{
				RegisteredStoresInTransaction[localIdentifier] =
					registered = new HashSet<string>();
			}

			if (registered.Add(StoreIdentifier))
			{
				var transactionalSession = (ITransactionalDocumentSession)this;
				if (documentStore.DatabaseCommands.SupportsPromotableTransactions == false)
				{
					Transaction.Current.EnlistDurable(
						ResourceManagerId,
						new RavenClientEnlistment(transactionalSession, () => RegisteredStoresInTransaction.Remove(localIdentifier)),
						EnlistmentOptions.None);
				}
				else
				{
					var promotableSinglePhaseNotification = new PromotableRavenClientEnlistment(transactionalSession,
																								() =>
																								RegisteredStoresInTransaction.
																									Remove(localIdentifier));
					var registeredSinglePhaseNotification =
						Transaction.Current.EnlistPromotableSinglePhase(promotableSinglePhaseNotification);

					if (registeredSinglePhaseNotification == false)
					{
						Transaction.Current.EnlistDurable(
							ResourceManagerId,
							new RavenClientEnlistment(transactionalSession, () => RegisteredStoresInTransaction.Remove(localIdentifier)),
							EnlistmentOptions.None);
					}
				}
			}
			hasEnlisted = true;
		}
#endif


		/// <summary>
		/// Mark the entity as read only, change tracking won't apply 
		/// to such an entity. This can be done as an optimization step, so 
		/// we don't need to check the entity for changes.
		/// </summary>
		public void MarkReadOnly(object entity)
		{
			GetMetadataFor(entity)[Constants.RavenReadOnly] = true;
		}


		/// <summary>
		/// Determines if the entity have changed.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <param name="documentMetadata">The document metadata.</param>
		/// <returns></returns>
		protected bool EntityChanged(object entity, DocumentMetadata documentMetadata)
		{
			if (documentMetadata == null)
				return true;

			string id;
			if (TryGetIdFromInstance(entity, out id) &&
				string.Equals(documentMetadata.Key, id, StringComparison.InvariantCultureIgnoreCase) == false)
				return true;

			// prevent saves of a modified read only entity
			if (documentMetadata.OriginalMetadata.ContainsKey(Constants.RavenReadOnly) &&
				documentMetadata.OriginalMetadata.Value<bool>(Constants.RavenReadOnly) &&
				documentMetadata.Metadata.ContainsKey(Constants.RavenReadOnly) &&
				documentMetadata.Metadata.Value<bool>(Constants.RavenReadOnly))
				return false;

			var newObj = ConvertEntityToJson(entity, documentMetadata.Metadata);
			return RavenJToken.DeepEquals(newObj, documentMetadata.OriginalValue) == false ||
				RavenJToken.DeepEquals(documentMetadata.Metadata, documentMetadata.OriginalMetadata) == false;
		}

		private RavenJObject ConvertEntityToJson(object entity, RavenJObject metadata)
		{
			var entityType = entity.GetType();
			var identityProperty = documentStore.Conventions.GetIdentityProperty(entityType);

			var objectAsJson = GetObjectAsJson(entity);
			if (identityProperty != null)
			{
				objectAsJson.Remove(identityProperty.Name);
			}

			SetClrType(entityType, metadata);

			foreach (var documentConversionListener in listeners.ConversionListeners)
			{
				documentConversionListener.EntityToDocument(entity, objectAsJson, metadata);
			}

			return objectAsJson;
		}

		private void SetClrType(Type entityType, RavenJObject metadata)
		{
			if (
#if !NET35
				entityType == typeof(DynamicJsonObject) ||
#endif
				entityType == typeof(RavenJObject)) // dynamic types
			{
				if (metadata.ContainsKey(Constants.RavenClrType))
					return;// do not overwrite the value
			}

			metadata[Constants.RavenClrType] = Conventions.GetClrTypeName(entityType);
		}

		private RavenJObject GetObjectAsJson(object entity)
		{
			var jObject = entity as RavenJObject;
			if (jObject != null)
				return jObject;

			if (cachedJsonDocs != null && cachedJsonDocs.TryGetValue(entity, out jObject))
				return jObject;

			var jsonSerializer = Conventions.CreateSerializer();
			jObject = RavenJObject.FromObject(entity, jsonSerializer);
			if (jsonSerializer.TypeNameHandling == TypeNameHandling.Auto)// remove the default types
			{
				var resolveContract = jsonSerializer.ContractResolver.ResolveContract(entity.GetType());
				TrySimplfyingJson(jObject, resolveContract);
			}

			if (cachedJsonDocs != null)
				cachedJsonDocs[entity] = jObject;
			return jObject;
		}

		private static void TrySimplfyingJson(RavenJObject jObject, JsonContract contract)
		{
			var objectContract = contract as JsonObjectContract;
			if (objectContract == null)
				return;

			var deferredActions = new List<Action>();
			foreach (var kvp in jObject)
			{
				var prop = kvp;
				if (prop.Value == null)
					continue;
				var obj = prop.Value as RavenJObject;
				if (obj == null)
					continue;

				var jsonProperty = objectContract.Properties.GetClosestMatchProperty(prop.Key);

				if (ShouldSimplfyJsonBasedOnType(obj.Value<string>("$type"), jsonProperty) == false)
					continue;

				if (obj.ContainsKey("$values") == false)
				{
					deferredActions.Add(() => obj.Remove("$type"));
				}
				else
				{
					deferredActions.Add(() => jObject[prop.Key] = obj["$values"]);
				}
			}
			foreach (var deferredAction in deferredActions)
			{
				deferredAction();
			}

			foreach (var prop in jObject.Where(prop => prop.Value != null))
			{
				switch (prop.Value.Type)
				{
					case JTokenType.Array:
						foreach (var item in ((RavenJArray)prop.Value))
						{
							var ravenJObject = item as RavenJObject;
							if (ravenJObject != null)
								TrySimplfyingJson(ravenJObject, contract);
						}
						break;
					case JTokenType.Object:
						TrySimplfyingJson((RavenJObject)prop.Value, contract);
						break;
				}
			}
		}

		private static bool ShouldSimplfyJsonBasedOnType(string typeValue, JsonProperty jsonProperty)
		{
			if (jsonProperty != null && (jsonProperty.TypeNameHandling == TypeNameHandling.All || jsonProperty.TypeNameHandling == TypeNameHandling.Arrays))
				return false; // explicitly rejected what we are trying to do here

			if (typeValue == null)
				return false;
			if (typeValue.StartsWith("System.Collections.Generic.List`1[["))
				return true;
			if (typeValue.StartsWith("System.Collections.Generic.Dictionary`2[["))
				return true;
			if (typeValue.EndsWith("[], mscorlib")) // array
				return true;
			return false;
		}


		/// <summary>
		/// All calls to convert an entity to a json object would be cache
		/// This is used inside the SaveChanges() action, where we need to access the entities json
		/// in several disparate places.
		/// 
		/// Note: This assumes that no modifications can happen during the SaveChanges. This is naturally true
		/// Note: for SaveChanges (and multi threaded access will cause undefined behavior anyway).
		/// Note: For SaveChangesAsync, the same holds true as well.
		/// </summary>
		protected IDisposable EntitiesToJsonCachingScope()
		{
			cachedJsonDocs = new Dictionary<object, RavenJObject>();

			return new DisposableAction(() => cachedJsonDocs = null);
		}


		/// <summary>
		/// Evicts the specified entity from the session.
		/// Remove the entity from the delete queue and stops tracking changes for this entity.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entity">The entity.</param>
		public void Evict<T>(T entity)
		{
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value))
			{
				entitiesAndMetadata.Remove(entity);
				entitiesByKey.Remove(value.Key);
			}
			deletedEntities.Remove(entity);
		}

		/// <summary>
		/// Clears this instance.
		/// Remove all entities from the delete queue and stops tracking changes for all entities.
		/// </summary>
		public void Clear()
		{
			entitiesAndMetadata.Clear();
			deletedEntities.Clear();
			entitiesByKey.Clear();
		}

		readonly List<ICommandData> deferedCommands = new List<ICommandData>();

		/// <summary>
		/// Defer commands to be executed on SaveChanges()
		/// </summary>
		/// <param name="commands">The commands to be executed</param>
		public virtual void Defer(params ICommandData[] commands)
		{
			deferedCommands.AddRange(commands);
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public virtual void Dispose()
		{

		}

		/// <summary>
		/// Commits the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public abstract void Commit(Guid txId);
		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public abstract void Rollback(Guid txId);
		/// <summary>
		/// Promotes the transaction.
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns></returns>
		public abstract byte[] PromoteTransaction(Guid fromTxId);

#if !SILVERLIGHT
		/// <summary>
		/// Clears the enlistment.
		/// </summary>
		protected void ClearEnlistment()
		{
			hasEnlisted = false;
		}
#endif
		/// <summary>
		/// Metadata held about an entity by the session
		/// </summary>
		public class DocumentMetadata
		{
			/// <summary>
			/// Gets or sets the original value.
			/// </summary>
			/// <value>The original value.</value>
			public RavenJObject OriginalValue { get; set; }
			/// <summary>
			/// Gets or sets the metadata.
			/// </summary>
			/// <value>The metadata.</value>
			public RavenJObject Metadata { get; set; }
			/// <summary>
			/// Gets or sets the ETag.
			/// </summary>
			/// <value>The ETag.</value>
			public Guid? ETag { get; set; }
			/// <summary>
			/// Gets or sets the key.
			/// </summary>
			/// <value>The key.</value>
			public string Key { get; set; }
			/// <summary>
			/// Gets or sets the original metadata.
			/// </summary>
			/// <value>The original metadata.</value>
			public RavenJObject OriginalMetadata { get; set; }

			/// <summary>
			/// A concurrency check will be forced on this entity 
			/// even if UseOptimisticConcurrency is set to false
			/// </summary>
			public bool ForceConcurrencyCheck { get; set; }
		}

		/// <summary>
		/// Data for a batch command to the server
		/// </summary>
		public class SaveChangesData
		{
			public SaveChangesData()
			{
				Commands = new List<ICommandData>();
				Entities = new List<object>();
			}

			/// <summary>
			/// Gets or sets the commands.
			/// </summary>
			/// <value>The commands.</value>
			public List<ICommandData> Commands { get; set; }

			public int DeferredCommandsCount { get; set; }

			/// <summary>
			/// Gets or sets the entities.
			/// </summary>
			/// <value>The entities.</value>
			public IList<object> Entities { get; set; }
		}

		protected void LogBatch(SaveChangesData data)
		{
			log.Debug(() =>
			{
				var sb = new StringBuilder()
					.AppendFormat("Saving {0} changes to {1}", data.Commands.Count, StoreIdentifier)
					.AppendLine();
				foreach (var commandData in data.Commands)
				{
					sb.AppendFormat("\t{0} {1}", commandData.Method, commandData.Key).AppendLine();
				}
				return sb.ToString();
			});
		}

		public void RegisterMissing(string id)
		{
			knownMissingIds.Add(id);
		}

		public void RegisterMissingIncludes(IEnumerable<RavenJObject> results, IEnumerable<string> includes)
		{
			if (includes == null || includes.Count() == 0)
				return;

			foreach (var result in results)
			{
				foreach (var include in includes)
				{
					IncludesUtil.Include(result, include, id =>
					{
						if(id == null)
							return;
						if (IsLoaded(id) == false)
							RegisterMissing(id);
					});
				}
			}
		}
	}
}
