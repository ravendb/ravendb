using System;
using System.Collections.Generic;
using Newtonsoft.Json.Serialization;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal;

internal class DefaultRavenSerializationBinder : DefaultSerializationBinder
{
    public static readonly DefaultRavenSerializationBinder Instance = new();

    private static HashSet<Type> ForbiddenTypesCache = new();

    private static HashSet<Type> SafeTypesCache = new();

    private static readonly HashSet<string> ForbiddenNamespaces = new() { "Microsoft.VisualStudio" };

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

    private DefaultRavenSerializationBinder()
    {
    }

    public override Type BindToType(string assemblyName, string typeName)
    {
        var deserializedType = base.BindToType(assemblyName, typeName);

        AssertType(deserializedType);

        return deserializedType;
    }

    public override void BindToName(Type serializedType, out string assemblyName, out string typeName)
    {
        AssertType(serializedType);

        base.BindToName(serializedType, out assemblyName, out typeName);
    }

    private static void AssertType(Type type)
    {
        if (type == null)
            return;

        if (SafeTypesCache.Contains(type))
            return;

        if (ForbiddenTypesCache.Contains(type))
            ThrowForbiddenType(type);

        if (ForbiddenNamespaces.Contains(type.Namespace))
        {
            UpdateCache(ref ForbiddenTypesCache, type);
            ThrowForbiddenNamespace(type);
        }

        if (ForbiddenTypes.Contains(type.FullName))
        {
            UpdateCache(ref ForbiddenTypesCache, type);
            ThrowForbiddenType(type);
        }

        UpdateCache(ref SafeTypesCache, type);
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
        throw new InvalidOperationException($"Cannot resolve type '{type.FullName}' because the type is on a blacklist due to security reasons. Please customize json deserializer in the conventions and override SerializationBinder with your own logic if you want to allow this type.");
    }

    private static void ThrowForbiddenNamespace(Type type)
    {
        throw new InvalidOperationException($"Cannot resolve type '{type.FullName}' because the namespace is on a blacklist due to security reasons. Please customize json deserializer in the conventions and override SerializationBinder with your own logic if you want to allow this type.");
    }
}
