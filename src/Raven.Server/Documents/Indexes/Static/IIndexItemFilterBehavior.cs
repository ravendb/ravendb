namespace Raven.Server.Documents.Indexes.Static
{
    public interface IIndexItemFilterBehavior
    {
        bool ShouldFilter(IndexItem item);
    }
}
