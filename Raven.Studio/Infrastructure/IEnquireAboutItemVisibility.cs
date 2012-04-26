using System;

namespace Raven.Studio.Infrastructure
{
    public interface IEnquireAboutItemVisibility
    {
        event EventHandler<QueryItemVisibilityEventArgs> QueryItemVisibility;
    }

    public class QueryItemVisibilityEventArgs : EventArgs
    {
        public QueryItemVisibilityEventArgs()
        {
        }

        public int? FirstVisibleIndex { get; private set; }

        public int? LastVisibleIndex { get; private set; }

        public void SetVisibleRange(int firstVisibleIndex, int lastVisibleIndex)
        {
            FirstVisibleIndex = FirstVisibleIndex.HasValue ? Math.Min(firstVisibleIndex, FirstVisibleIndex.Value) : firstVisibleIndex;
            LastVisibleIndex = LastVisibleIndex.HasValue ? Math.Max(lastVisibleIndex, LastVisibleIndex.Value) : lastVisibleIndex;
        }
    }
}