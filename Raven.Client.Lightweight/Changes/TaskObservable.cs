using System;
using System.Threading.Tasks;

namespace Raven.Client.Changes
{
	public interface IObservableWithTask<out T> : IObservable<T>
	{
		Task Task { get; }
	}
}