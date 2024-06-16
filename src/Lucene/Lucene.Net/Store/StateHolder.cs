using System.Threading;

namespace Lucene.Net.Store
{
    public static class StateHolder
    {
        public static AsyncLocal<IState> Current = new AsyncLocal<IState>();
    }
}