using System;
using System.Linq;
using System.Text.RegularExpressions;
using Lucene.Net.Documents;
using Raven.Abstractions.Extensions;
using Raven.Bundles.IndexReplicationToRedis.Data;
using Raven.Database.Plugins;
using ServiceStack.Common.Utils;
using ServiceStack.Redis;
using Document = Lucene.Net.Documents.Document;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Globalization;  

namespace Raven.Bundles.IndexReplicationToRedis
{
    public class IndexReplicationToRedisIndexUpdateTrigger : AbstractIndexUpdateTrigger
    {
        public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
        {
            var document = Database.Get("Raven/IndexReplicationToRedis/" + indexName, null);

            if (document == null)
                return null;

			var destination = document.DataAsJson.JsonDeserialization<IndexReplicationToRedisDestination>();

            if (String.IsNullOrEmpty(destination.Server))
                throw new InvalidOperationException("You must indicate the Server property of the IndexReplicationToRedisDestination object");

			if (String.IsNullOrEmpty(destination.PocoTypeAssemblyQualifiedName) && destination.RedisSaveMode == IndexReplicationToRedisMode.PocoType)
				throw new InvalidOperationException("You must indicate the PocoTypeAssemblyQualifiedName property of the IndexReplicationToRedisDestination object");

			if ((destination.FieldsToHashSave == null || destination.FieldsToHashSave.Count == 0) && destination.RedisSaveMode == IndexReplicationToRedisMode.RedisHash)
				throw new InvalidOperationException("You must indicate the FieldsToHashSave property of the IndexReplicationToRedisDestination object");

            return new ReplicateToRedisIndexUpdateBatcher(destination);
        }

        public class ReplicateToRedisIndexUpdateBatcher : AbstractIndexUpdateTriggerBatcher
        {
			private static ConcurrentDictionary<string, Type> loadedTypes = new ConcurrentDictionary<string, Type>();
			private static Regex datePattern = new Regex(@"\d{17}", RegexOptions.Compiled);

			private readonly IndexReplicationToRedisDestination destination;
            
            private IRedisTransaction redisTransaction;
			private IRedisClient redisClient;

			private bool lastOperationCompleted;

            public ReplicateToRedisIndexUpdateBatcher(IndexReplicationToRedisDestination destination)
            {
                this.destination = destination;
            }

			//OVERRIDEN

            public override void OnIndexEntryCreated(string entryKey, Document document)
            {
				lastOperationCompleted = false;

				if(destination.RedisSaveMode == IndexReplicationToRedisMode.RedisHash)
					SaveUsingRedisHashMode(document, entryKey);
				else
					SaveUsingPocoTypeMode(document, entryKey);

				lastOperationCompleted = true;
            }

            public override void OnIndexEntryDeleted(string entryKey)
            {
				lastOperationCompleted = false;

				GetRedisTransaction().QueueCommand(r =>
                {
                    r.RemoveEntry(GetRedisUrn(entryKey));
                });

				lastOperationCompleted = true;
            }

            public override void Dispose()
            {
				if (redisTransaction == null)
					return;

				if (lastOperationCompleted)
					redisTransaction.Commit();
				else
					redisTransaction.Rollback();

                redisTransaction.Dispose();
				redisClient.Dispose();
            }

			//PRIVATE METHODS

			private void SaveUsingPocoTypeMode(Document document, string entryKey)
			{
				var pocoType = GetPocoType(destination.PocoTypeAssemblyQualifiedName);

				var mappingInstance = Activator.CreateInstance(pocoType);
				var properties = pocoType.GetProperties().ToList();

				if (properties.FirstOrDefault(p => p.Name == IdUtils.IdField && p.PropertyType == typeof(string)) == null)
					throw new InvalidOperationException(String.Format("The type {0} must hava a public {1} string property to keep the document key"
						, pocoType.Name, IdUtils.IdField));

				foreach (var propertyInfo in properties)
				{
					if (propertyInfo.Name == IdUtils.IdField)
					{
						propertyInfo.SetValue(mappingInstance
							, entryKey
							, null);

						continue;
					}

					propertyInfo.SetValue(mappingInstance
						, GetObjectFromField(document, propertyInfo.Name)
						, null);
				}

				GetRedisTransaction().QueueCommand(r =>
				{
					var storeMethodInfo = r.GetType().GetMethod("Store");
					var genericMethod = storeMethodInfo.MakeGenericMethod(pocoType);

					genericMethod.Invoke(r, new[] { mappingInstance });
				});
			}

			private void SaveUsingRedisHashMode(Document document, string entryKey)
			{
				var dictionary = new Dictionary<string, string>();

				destination.FieldsToHashSave.ForEach(fieldName =>
						dictionary.Add(fieldName, GetStringFromField(document, fieldName))
						);

				GetRedisTransaction().QueueCommand(r => r.SetRangeInHash(GetRedisUrn(entryKey), dictionary));
			}

			private Type GetPocoType(string pocoTypeAssemblyQualifiedName)
			{
				Type type;

				if (loadedTypes.TryGetValue(pocoTypeAssemblyQualifiedName, out type))
					return type;

				type = Type.GetType(pocoTypeAssemblyQualifiedName);

				loadedTypes.TryAdd(pocoTypeAssemblyQualifiedName, type);

				return type;
			}

            private IRedisTransaction GetRedisTransaction()
            {
                if (redisTransaction != null)
                    return redisTransaction;

                redisClient = new RedisClient(destination.Server);
				redisTransaction = redisClient.CreateTransaction();

                return redisTransaction;
            }

            private string GetRedisUrn(string entryKey)
            {
				if (destination.RedisSaveMode == IndexReplicationToRedisMode.RedisHash)
					return entryKey;
				
				return String.Format("urn:{0}:{1}", GetPocoType(destination.PocoTypeAssemblyQualifiedName), entryKey, CultureInfo.CurrentCulture);
            }

            private  object GetObjectFromField(Document document, string propertyName)
            {
				var fieldable    = document.GetFieldable(propertyName);
                var numericfield = document.GetFieldable(String.Concat(propertyName, "_Range"));

                if (numericfield != null)
                    return ((NumericField)numericfield).GetNumericValue();

				if (fieldable == null)
					return null;

                var stringValue = fieldable.StringValue();

                if (datePattern.IsMatch(stringValue))
                {
                    try
                    {
	                    return DateTools.StringToDate(stringValue);
                    }
                    catch { }
                }

                return stringValue;
            }

			private string GetStringFromField(Document document, string propertyName)
			{
				var fieldable    = document.GetFieldable(propertyName);
				var numericfield = document.GetFieldable(String.Concat(propertyName, "_Range"));

				if (numericfield != null)
					return String.Format(CultureInfo.InvariantCulture, "{0}", ((NumericField) numericfield).GetNumericValue());

				if (fieldable == null)
					return String.Empty;

				var stringValue = fieldable.StringValue();

				if (datePattern.IsMatch(stringValue))
				{
					try
					{
						return DateTools.StringToDate(stringValue).ToString("u");
					}
					catch { }
				}

				return stringValue;
			}

        }
    }
}
