using System;

namespace Sparrow.Json
{
    /// <summary>
    /// Instructs the <see cref="JsonDeserializationBase"/> to serialize nonPublic field .
    /// </summary>
    [AttributeUsage(AttributeTargets.Field )]
    public sealed class JsonDeserializationDoNotIgnoreAttribute : Attribute
    {
    }
}
