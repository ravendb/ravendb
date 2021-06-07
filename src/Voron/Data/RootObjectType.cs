namespace Voron.Data
{
    public enum RootObjectType : byte
    {
        None = 0,
        VariableSizeTree = 1,
        EmbeddedFixedSizeTree = 2,
        FixedSizeTree = 3,
        Reserved = 4,
        Table = 5,
        Set = 6,
    }
}
