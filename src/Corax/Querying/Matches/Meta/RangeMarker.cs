namespace Corax.Querying.Matches.Meta;

public static class Range
{
    public interface Marker { }
    public struct Exclusive : Marker { }
    public struct Inclusive : Marker { }
}
