namespace Raven.Server.Documents.Sharding.Streaming;

public class ShardStreamItem<T>
{
    public T Item;
    public int ShardNumber;
}
