using System;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using Microsoft.CSharp.RuntimeBinder;

using Raven.NewClient.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json.Serialization;

namespace Raven.NewClient.Client.Document
{
    public class GenerateEntityIdOnTheClient
    {
        private readonly DocumentConvention conventions;
        private readonly Func<object, string> generateKey;

        public GenerateEntityIdOnTheClient(DocumentConvention conventions, Func<object, string> generateKey)
        {
            this.conventions = conventions;
            this.generateKey = generateKey;
        }

        private MemberInfo GetIdentityProperty(Type entityType)
        {
            return conventions.GetIdentityProperty(entityType);
        }

        /// <summary>
        /// Attempts to get the document key from an instance 
        /// </summary>
        public bool TryGetIdFromInstance(object entity, out string id)
        {
            if (entity == null)
                throw new ArgumentNullException("entity");
            var identityProperty = GetIdentityProperty(entity.GetType());
            if (identityProperty != null)
            {
                var value = identityProperty.GetValue(entity);
                return GetIdAsString(entity, value, out id);
            }
           
         id = null;
            return false;
        }

        private bool GetIdAsString(object entity, object value, out string id)
        {
            id = value as string;
            if (id == null && value != null) // need conversion
                id = conventions.FindFullDocumentKeyFromNonStringIdentifier(value, entity.GetType(), true);

            return id != null;
        }

        /// <summary>
        /// Tries to get the identity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public string GetOrGenerateDocumentKey(object entity)
        {
            string id;
            TryGetIdFromInstance(entity, out id);

            if (id == null)
            {
                // Generate the key up front
                id = generateKey(entity);
            }

            if (id != null && id.StartsWith("/"))
                throw new InvalidOperationException("Cannot use value '" + id + "' as a document id because it begins with a '/'");
            return id;
        }

        public string GenerateDocumentKeyForStorage(object entity)
        {
            string id;
            if (entity is IDynamicMetaObjectProvider)
            {
                if (TryGetIdFromDynamic(entity, out id) == false || id == null)
                {
                    id = generateKey(entity);
                    // If we generated a new id, store it back into the Id field so the client has access to to it                    
                    if (id != null)
                        TrySetIdOnDynamic(entity, id);
                }
                return id;
            }

            id = GetOrGenerateDocumentKey(entity);
            TrySetIdentity(entity, id);
            return id;
        }

        public bool TryGetIdFromDynamic(dynamic entity, out string id)
        {
            try
            {
                object value = entity.Id;
               return GetIdAsString(entity, value, out id);
            }
            catch (RuntimeBinderException)
            {
                id = null;
                return false;
            }
        }

        /// <summary>
        /// Tries to set the identity property
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <param name="id">The id.</param>
        protected internal void TrySetIdentity(object entity, string id)
        {
            var entityType = entity.GetType();
            var identityProperty = conventions.GetIdentityProperty(entityType);
            if (identityProperty == null)
            {
                if (entity is IDynamicMetaObjectProvider)
                {
                    
                    TrySetIdOnDynamic(entity, id);
                }
                return;
            }

            if (identityProperty.CanWrite())
            {
                SetPropertyOrField(identityProperty.Type(), entity, val => identityProperty.SetValue(entity, val), id);
            }
            else
            {
                const BindingFlags privateInstanceField = BindingFlags.Instance | BindingFlags.NonPublic;
                var fieldInfo = entityType.GetField("<" + identityProperty.Name + ">i__Field", privateInstanceField) ??
                                entityType.GetField("<" + identityProperty.Name + ">k__BackingField", privateInstanceField);

                if (fieldInfo == null)
                    return;

                SetPropertyOrField(identityProperty.Type(), entity, val => fieldInfo.SetValue(entity, val), id);
            }
        }

        public void TrySetIdOnDynamic(dynamic entity, string id)
        {
            try
            {
                entity.Id = id;
            }
            catch (RuntimeBinderException)
            {
            }
        }

        private void SetPropertyOrField(Type propertyOrFieldType, object entity, Action<object> setIdentifier, string id)
        {
            if (propertyOrFieldType == typeof(string))
            {
                setIdentifier(id);
            }
            else // need converting
            {
                var converter =
                    conventions.IdentityTypeConvertors.FirstOrDefault(x => x.CanConvertFrom(propertyOrFieldType));
                if (converter == null)
                    throw new ArgumentException("Could not convert identity to type " + propertyOrFieldType +
                                                " because there is not matching type converter registered in the conventions' IdentityTypeConvertors");

                setIdentifier(converter.ConvertTo(conventions.FindIdValuePartForValueTypeConversion(entity, id)));
            }
        }
    }
}
