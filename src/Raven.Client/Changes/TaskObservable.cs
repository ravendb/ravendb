using System;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Changes
{
    public interface IObservableWithTask<T> : IObservable<T>
    {
        Task<IObservable<T>> Task { get; }
    }
}
