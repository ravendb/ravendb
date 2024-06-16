using Lucene.Net.Store;

namespace Lucene.Net.Util
{
    public interface ILuceneCloneable
    {
        object Clone(IState state);
    }
}