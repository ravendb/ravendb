using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization.Metadata;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal;

internal sealed class MissingPropertiesShape
{
    private readonly int[] _buckets;
    private readonly Member[] _members;

    public readonly bool IsObject;

    public readonly bool OwnsUnknownProperties;

    public readonly JsonPropertyMap Properties;

    public MissingPropertiesShape(JsonTypeInfo typeInfo, JsonPropertyMap properties)
    {
        IsObject = typeInfo.Kind == JsonTypeInfoKind.Object;
        OwnsUnknownProperties = IsObject && RavenJsonTypeInfoResolver.OwnsUnknownProperties(typeInfo);
        Properties = properties;

        int size = 1;
        while (size < properties.Count * 2)
            size <<= 1;

        _buckets = new int[size];
        _members = new Member[properties.Count];
#if NET8_0_OR_GREATER
        Array.Fill(_buckets, -1);
#else
        for (int i = 0; i < _buckets.Length; i++)
            _buckets[i] = -1;
#endif

        int index = 0;
        foreach (KeyValuePair<string, JsonPropertyInfo> property in properties.ByName)
            AddMember(ref index, property.Key, property.Value);

        if (properties.Shadowed != null)
        {
            foreach (KeyValuePair<string, JsonPropertyInfo> property in properties.Shadowed)
                AddMember(ref index, property.Key, property.Value);
        }
    }

    private void AddMember(ref int index, string propertyName, JsonPropertyInfo property)
    {
        byte[] name = Encoding.UTF8.GetBytes(propertyName);
        int bucket = GetBucket(name);
        _members[index] = new Member
        {
            Name = name,
            Property = property,
            CanHoldMissingProperties = CanHoldMissingProperties(property),
            Next = _buckets[bucket]
        };
        _buckets[bucket] = index++;
    }

    public static bool CanHoldMissingProperties(JsonPropertyInfo property)
    {
        return property.Get != null &&
               property.PropertyType.IsValueType == false &&
               property.PropertyType != typeof(string);
    }

    public bool TryGetMember(ReadOnlySpan<byte> name, out JsonPropertyInfo property, out bool canHoldMissingProperties)
    {
        for (int i = _buckets[GetBucket(name)]; i != -1; i = _members[i].Next)
        {
            ref Member member = ref _members[i];
            if (name.SequenceEqual(member.Name) == false)
                continue;

            property = member.Property;
            canHoldMissingProperties = member.CanHoldMissingProperties;
            return true;
        }

        property = null;
        canHoldMissingProperties = false;
        return false;
    }

    private int GetBucket(ReadOnlySpan<byte> name)
    {
        uint hash = 2166136261;
        for (int i = 0; i < name.Length; i++)
            hash = (hash ^ name[i]) * 16777619;

        return (int)(hash & (uint)(_buckets.Length - 1));
    }

    private struct Member
    {
        public byte[] Name;

        public JsonPropertyInfo Property;

        public bool CanHoldMissingProperties;

        public int Next;
    }
}
