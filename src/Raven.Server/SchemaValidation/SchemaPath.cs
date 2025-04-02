using System.Diagnostics;

namespace Raven.Server.SchemaValidation;

public readonly struct SchemaPath
{
    public static SchemaPath Root = new SchemaPath();
    private const string RootStr = "#";
    
    public string Property { get; private init; } = null;
    public string FullPath { get; private init; } = RootStr;

    public SchemaPath()
    {
        
    }
    
    public override string ToString() => FullPath;

    public static SchemaPath operator +(SchemaPath me, string property)
    {
        return new SchemaPath
        {
            Property = property,
            FullPath = $"{me.FullPath}/{property}"
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
