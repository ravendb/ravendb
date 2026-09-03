using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Client.Json.Serialization.SystemTextJson
{
    internal sealed class RavenSerializationBinder
    {
        public static readonly RavenSerializationBinder Instance = new();

        private HashSet<Type> _forbiddenTypesCache = new();

        private HashSet<Type> _safeTypesCache = new();

        private readonly HashSet<string> _forbiddenNamespaces = new() { "Microsoft.VisualStudio" };

        private bool _used;

        /// <summary>
        /// https://cheatsheetseries.owasp.org/cheatsheets/Deserialization_Cheat_Sheet.html#known-net-rce-gadgets
        /// </summary>
        private static readonly HashSet<string> ForbiddenTypes = new()
        {
            "System.Configuration.Install.AssemblyInstaller",
            "System.Activities.Presentation.WorkflowDesigner",
            "System.Windows.ResourceDictionary",
            "System.Windows.Data.ObjectDataProvider",
            "System.Windows.Forms.BindingSource",
            "Microsoft.Exchange.Management.SystemManager.WinForms.ExchangeSettingsProvider",
            "System.Data.DataViewManager",
            "System.Xml.XmlDocument",
            "System.Xml.XmlDataDocument",
            "System.Management.Automation.PSObject"
        };

        public void ValidateType(Type type)
        {
            _used = true;
            AssertType(type);
        }

        public void RegisterForbiddenNamespace(string @namespace)
        {
            if (@namespace == null)
                throw new ArgumentNullException(nameof(@namespace));

            AssertNotUsed();

            _forbiddenNamespaces.Add(@namespace);
        }

        public void RegisterForbiddenType(Type type)
        {
            AssertNotUsed();

            UpdateCache(ref _forbiddenTypesCache, type);
        }

        public void RegisterSafeType(Type type)
        {
            AssertNotUsed();

            UpdateCache(ref _safeTypesCache, type);
        }

        private void AssertType(Type type)
        {
            if (type == null)
                return;

            if (_safeTypesCache.Contains(type))
                return;

            if (_forbiddenTypesCache.Contains(type))
                ThrowForbiddenType(type);

            if (type.Namespace != null && _forbiddenNamespaces.Any(@namespace => type.Namespace.StartsWith(@namespace)))
            {
                UpdateCache(ref _forbiddenTypesCache, type);
                ThrowForbiddenNamespace(type);
            }

            if (ForbiddenTypes.Contains(type.FullName))
            {
                UpdateCache(ref _forbiddenTypesCache, type);
                ThrowForbiddenType(type);
            }

            if (type.IsGenericType)
            {
                Type[] genericArguments = type.GetGenericArguments();
                for (int i = 0; i < genericArguments.Length; i++)
                {
                    try
                    {
                        AssertType(genericArguments[i]);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException(
                            "Generic type: " + type.FullName + ", contains a generic argument " +
                            genericArguments[i].FullName + " that is blocked from serialization", e);
                    }
                }
            }

            UpdateCache(ref _safeTypesCache, type);
        }

        private void AssertNotUsed()
        {
            if (_used)
                throw new InvalidOperationException("Cannot perform this operation, because binder was already used.");
        }

        private static void UpdateCache(ref HashSet<Type> cache, Type type)
        {
            cache = new HashSet<Type>(cache)
            {
                type
            };
        }

        private static void ThrowForbiddenType(Type type)
        {
            throw new InvalidOperationException(
                $"Cannot resolve type '{type.FullName}' because the type is on a blacklist due to security reasons. " +
                "Please customize json deserializer in the conventions and override the serialization binder with your own logic if you want to allow this type.");
        }

        private static void ThrowForbiddenNamespace(Type type)
        {
            throw new InvalidOperationException(
                $"Cannot resolve type '{type.FullName}' because the namespace is on a blacklist due to security reasons. " +
                "Please customize json deserializer in the conventions and override the serialization binder with your own logic if you want to allow this type.");
        }
    }
}
