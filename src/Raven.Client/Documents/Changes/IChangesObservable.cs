using System;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Changes
{
    public interface IChangesObservable<out T> : IObservable<T>
    {
        Task EnsureSubscribedNow();
    }
}