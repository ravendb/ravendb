using System;
using System.Dynamic;
using System.Reflection;
using Microsoft.CSharp.RuntimeBinder;

using Raven.NewClient.Abstractions.Extensions;

namespace Raven.NewClient.Client.Document
{
    public class GenerateEntityIdOnTheClient
    {
        private readonly DocumentConvention _conventions;
        private readonly Func<object, string> _generateKey;

        public GenerateEntityIdOnTheClient(DocumentConvention conventions, Func<object, string> generateKey)
        {
            _conventions = conventions;
            _generateKey = generateKey;
        }

        private MemberInfo GetIdentityProperty(Type entityType)
        {
            return _conventions.GetIdentityProperty(entityType);
        }

        /// <summary>
        /// Attempts to get the document key from an instance 
        /// </summary>
        public bool TryGetIdFromInstance(object entity, out string id)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));
            var identityProperty = GetIdentityProperty(entity.GetType());
            if (identityProperty != null)
            {
                var value = identityProperty.GetValue(entity);
                id = value as string;
                return id != null;
            }

            id = null;
            return false;
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
                id = _generateKey(entity);
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
                    id = _generateKey(entity);
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
                id = value as string;
                return value != null;
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
            var identityProperty = _conventions.GetIdentityProperty(entityType);
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
                SetPropertyOrField(identityProperty, entity, val => identityProperty.SetValue(entity, val), id);
            }
            else
            {
                const BindingFlags privateInstanceField = BindingFlags.Instance | BindingFlags.NonPublic;
                var fieldInfo = entityType.GetField("<" + identityProperty.Name + ">i__Field", privateInstanceField) ??
                                entityType.GetField("<" + identityProperty.Name + ">k__BackingField", privateInstanceField);

                if (fieldInfo == null)
                    return;

                SetPropertyOrField(identityProperty, entity, val => fieldInfo.SetValue(entity, val), id);
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

        private static void SetPropertyOrField(MemberInfo memberInfo, object entity, Action<object> setIdentifier, string id)
        {
            if (memberInfo.Type() != typeof(string))
            {
                var isProperty = memberInfo.IsProperty();
                var name = isProperty ? "property" : "field";
                throw new NotSupportedException($"Cannot set identity value '{id}' on {name} '{memberInfo.Name}' for type '{entity.GetType().FullName}' because {name} type is not a string.");
            }

            setIdentifier(id);
        }
    }
}
