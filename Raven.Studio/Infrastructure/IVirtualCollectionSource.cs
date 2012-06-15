using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading.Tasks;

namespace Raven.Studio.Infrastructure
{
    public interface IVirtualCollectionSource<T>
    {
        event EventHandler<VirtualCollectionSourceChangedEventArgs> CollectionChanged;

        int Count { get; }

        Task<IList<T>> GetPageAsync(int start, int pageSize, IList<SortDescription> sortDescriptions);

        void Refresh(RefreshMode mode);
    }

    public class VirtualCollectionSourceChangedEventArgs : EventArgs
    {
        public ChangeType ChangeType { get; private set; }

        public VirtualCollectionSourceChangedEventArgs(ChangeType changeType)
        {
            ChangeType = changeType;
        }
    }

    public enum ChangeType
    {
        /// <summary>
        /// Current data is invalid and should be cleared
        /// </summary>
        Reset,
        /// <summary>
        /// Current data may still be valid, and can be shown whilst refreshing
        /// </summary>
        Refresh,
    }
}
