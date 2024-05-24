using System;
using System.Dynamic;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.CSharp.RuntimeBinder;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Util;

namespace Raven.Client.Documents.Identity
{
    public class GenerateEntityIdOnTheClient
    {
        private readonly DocumentConventions _conventions;
        private readonly Func<object, string> _generateId;
        private readonly Func<object, Task<string>> _generateIdAsync;

        [Obsolete("This constructor is not supported anymore. Will be removed in next major version of the product. Use constructor with 'generateIdAsync' parameter instead.")]
        public GenerateEntityIdOnTheClient(DocumentConventions conventions, Func<object, string> generateId)
        {
            _conventions = conventions;
            _generateIdAsync = entity => Task.FromResult(generateId(entity));
            _generateId = generateId;
        }

        public GenerateEntityIdOnTheClient(DocumentConventions conventions, Func<object, Task<string>> generateIdAsync)
        {
            _conventions = conventions;
            _generateIdAsync = generateIdAsync;

            if (_generateIdAsync != null)
                _generateId = entity => AsyncHelpers.RunSync(() => _generateIdAsync(entity));
        }

        private MemberInfo GetIdentityProperty(Type entityType)
        {
            return _conventions.GetIdentityProperty(entityType);
        }

        /// <summary>
        /// Attempts to get the document ID from an instance 
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
        public string GetOrGenerateDocumentId(object entity)
        {
            TryGetIdFromInstance(entity, out string id);

            id ??= _generateId(entity);

            if (id != null && id.StartsWith("/"))
                throw new InvalidOperationException("Cannot use value '" + id + "' as a document id because it begins with a '/'");
            return id;
        }

        public async ValueTask<string> GetOrGenerateDocumentIdAsync(object entity)
        {
            TryGetIdFromInstance(entity, out string id);

            id ??= await _generateIdAsync(entity).ConfigureAwait(false);

            if (id != null && id.StartsWith("/"))
                throw new InvalidOperationException("Cannot use value '" + id + "' as a document id because it begins with a '/'");
            return id;
        }

        public string GenerateDocumentIdForStorage(object entity)
        {
            string id;
            if (_conventions.AddIdFieldToDynamicObjects && entity is IDynamicMetaObjectProvider)
            {
                if (TryGetIdFromDynamic(entity, out id) && id != null)
                    return id;

                id = _generateId(entity);
                // If we generated a new id, store it back into the Id field so the client has access to it                    
                if (id != null)
                    TrySetIdOnDynamic(entity, id);
                return id;
            }

            id = GetOrGenerateDocumentId(entity);
            TrySetIdentity(entity, id);
            return id;
        }

        public async ValueTask<string> GenerateDocumentIdForStorageAsync(object entity)
        {
            string id;
            if (_conventions.AddIdFieldToDynamicObjects && entity is IDynamicMetaObjectProvider)
            {
                if (TryGetIdFromDynamic(entity, out id) && id != null)
                    return id;

                id = await _generateIdAsync(entity).ConfigureAwait(false);
                // If we generated a new id, store it back into the Id field so the client has access to it                    
                if (id != null)
                    TrySetIdOnDynamic(entity, id);
                return id;
            }

            id = await GetOrGenerateDocumentIdAsync(entity).ConfigureAwait(false);
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
            TrySetIdentityInternal(entity, id, isProjection: false);
        }

        internal void TrySetIdentity(object entity, string id, bool isProjection)
        {
            TrySetIdentityInternal(entity, id, isProjection);
        }

        private void TrySetIdentityInternal(object entity, string id, bool isProjection)
        {
            var entityType = entity.GetType();
            var identityProperty = _conventions.GetIdentityProperty(entityType);
            if (identityProperty == null)
            {
                if (_conventions.AddIdFieldToDynamicObjects && entity is IDynamicMetaObjectProvider)
                {
                    TrySetIdOnDynamic(entity, id);
                }

                return;
            }

            if (identityProperty.CanWrite())
            {
                if (isProjection && identityProperty.GetValue(entity) != null)
                {
                    // identity property was already set
                    return;
                }

                SetPropertyOrField(identityProperty, entity, val => identityProperty.SetValue(ref entity, val), id);
            }
            else
            {
                const BindingFlags privateInstanceField = BindingFlags.Instance | BindingFlags.NonPublic;
                var fieldInfo = entityType.GetField("<" + identityProperty.Name + ">i__Field", privateInstanceField) ??
                                entityType.GetField("<" + identityProperty.Name + ">k__BackingField", privateInstanceField);

                if (fieldInfo == null)
                    return;

                if (isProjection && fieldInfo.GetValue(entity) != null)
                {
                    // identity property was already set
                    return;
                }

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
                // it is fine if the document doesn't
                // contain this property or if we can't set 
                // it. We can live without it. 
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
