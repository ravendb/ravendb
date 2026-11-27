using System;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Raven.Server.Json.Converters
{
    internal static class DeserializationStackGuard
    {
        public static void Register(JsonSerializerOptions options)
        {
            if (options.TypeInfoResolver is not DefaultJsonTypeInfoResolver resolver)
                throw new InvalidOperationException($"Expected a {nameof(DefaultJsonTypeInfoResolver)} but got {options.TypeInfoResolver?.GetType()}");

            resolver.Modifiers.Add(GuardObjects);
        }

        private static void GuardObjects(JsonTypeInfo typeInfo)
        {
            if (typeInfo.Kind != JsonTypeInfoKind.Object)
                return;

            Action<object> onDeserializing = typeInfo.OnDeserializing;
            typeInfo.OnDeserializing = value =>
            {
                RuntimeHelpers.EnsureSufficientExecutionStack();
                onDeserializing?.Invoke(value);
            };
        }
    }
}
