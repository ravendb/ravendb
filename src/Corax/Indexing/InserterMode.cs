namespace Corax.Indexing;

public enum InserterMode : byte
{
    ExactInsert = 0,
    Numerical = 1,
    Ignore = 1 << 1
}
