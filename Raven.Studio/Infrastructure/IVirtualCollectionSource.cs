using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading.Tasks;

namespace Raven.Studio.Infrastructure
{
    public interface IVirtualCollectionSource<T>
    {
        event EventHandler<VirtualCollectionChangedEventArgs> CollectionChanged;
        event EventHandler<DataFetchErrorEventArgs> DataFetchError;

        int Count { get; }

        Task<IList<T>> GetPageAsync(int start, int pageSize, IList<SortDescription> sortDescriptions);

        void Refresh();
    }
}
