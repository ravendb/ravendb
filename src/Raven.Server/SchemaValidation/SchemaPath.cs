using System.Diagnostics;

namespace Raven.Server.SchemaValidation;

public readonly struct SchemaPath
{
    public string Property { get; private init; }
    public string FullPath { get; private init; }

    public override string ToString() => FullPath;

    public static SchemaPath operator +(SchemaPath me, string property)
    {
        return new SchemaPath
        {
            Property = property,
            FullPath = string.IsNullOrEmpty(me.FullPath) ? property : $"{me.FullPath}.{property}"
        };
    }
    
    public static SchemaPath operator +(SchemaPath me, int index)
    {
        Debug.Assert(string.IsNullOrEmpty(me.FullPath) == false);
        return new SchemaPath
        {
            Property = $"[{index}]",
            FullPath = $"{me.FullPath}[{index}]"
        };
    }
}
