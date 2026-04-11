using System;
using System.Dynamic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal
{
    internal abstract class BlittableJsonConverterBase : IBlittableJsonConverterBase
    {
        protected readonly ISerializationConventions Conventions;

        protected BlittableJsonConverterBase(ISerializationConventions conventions)
        {
            Conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        }

        public void PopulateEntity(object entity, BlittableJsonReaderObject json)
        {
            var jsonSerializer = Conventions.CreateSerializer();
            PopulateEntity(entity, json, jsonSerializer);
        }

        public void PopulateEntity(object entity, BlittableJsonReaderObject json, IJsonSerializer jsonSerializer)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));
            if (json == null)
                throw new ArgumentNullException(nameof(json));
            if (jsonSerializer == null)
                throw new ArgumentNullException(nameof(jsonSerializer));

            var serializer = (NewtonsoftJsonJsonSerializer)jsonSerializer;
            var old = serializer.ObjectCreationHandling;
            serializer.ObjectCreationHandling = ObjectCreationHandling.Replace;

            try
            {
                using (var reader = new BlittableJsonReader())
                {
                    reader.Initialize(json);

                    serializer.Populate(reader, entity);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not populate entity.", ex);
            }
            finally
            {
                serializer.ObjectCreationHandling = old;
            }
        }

        protected static BlittableJsonReaderObject ToBlittableInternal(
             object entity,
             DocumentConventions conventions,
             JsonOperationContext context,
             IJsonSerializer serializer,
             IJsonWriter writer,
             bool removeIdentityProperty = true)
        {
            var usesDefaultContractResolver = ((JsonSerializer)serializer).ContractResolver.GetType() == typeof(DefaultRavenContractResolver);
            var type = entity.GetType();
            var isDynamicObject = entity is IDynamicMetaObjectProvider;
            var willUseDefaultContractResolver = usesDefaultContractResolver && isDynamicObject == false;
            var hasIdentityProperty = conventions.GetIdentityProperty(type) != null;

            if (willUseDefaultContractResolver)
            {
                DefaultRavenContractResolver.RootEntity = removeIdentityProperty && hasIdentityProperty ? entity : null;
                DefaultRavenContractResolver.RemovedIdentityProperty = false;

                // PERF: By moving the try..finally statement we forgo the need for prolog and epilog when it is not needed.
                try
                {
                    serializer.Serialize(writer, entity);
                }
                finally
                {
                    DefaultRavenContractResolver.RootEntity = null;
                }
            }
            else
            {
                serializer.Serialize(writer, entity);
            }

            writer.FinalizeDocument();

            var reader = writer.CreateReader();

            if (willUseDefaultContractResolver == false || hasIdentityProperty && DefaultRavenContractResolver.RemovedIdentityProperty == false)
            {
                //This is to handle the case when user defined it's own contract resolver
                //or we are serializing dynamic object

                var changes = removeIdentityProperty && BlittableJsonConverterHelper.TryRemoveIdentityProperty(reader, type, conventions, isDynamicObject);
                changes |= BlittableJsonConverterHelper.TrySimplifyJson(reader, type, ShouldSkipSimplification);

                if (changes)
                {
                    using (var old = reader)
                    {
                        reader = context.ReadObject(reader, "convert/entityToBlittable");
                    }
                }
            }

            return reader;
        }

        private static bool ShouldSkipSimplification(Type propertyType)
        {
            return propertyType == typeof(JObject) || propertyType == typeof(JArray) || propertyType == typeof(JValue);
        }
    }
}
