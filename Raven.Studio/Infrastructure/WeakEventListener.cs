//-----------------------------------------------------------------------
// <copyright file="WeakEventListener.cs" company="Microsoft">
//      (c) Copyright Microsoft Corporation.
//      This source is subject to the Microsoft Public License (Ms-PL).
//      Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
//      All other rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Studio.Infrastructure
{
	/// <summary>
	/// Implements a weak event listener that allows the owner to be garbage
	/// collected if its only remaining link is an event handler.
	/// </summary>
	/// <typeparam name="TInstance">Type of instance listening for the event.</typeparam>
	/// <typeparam name="TSource">Type of source for the event.</typeparam>
	/// <typeparam name="TEventArgs">Type of event arguments for the event.</typeparam>
	public class WeakEventListener<TInstance, TSource, TEventArgs> : IWeakEventListener where TInstance : class
	{
		/// <summary>
		/// WeakReference to the instance listening for the event.
		/// </summary>
		private WeakReference _weakInstance;

		/// <summary>
		/// Gets or sets the method to call when the event fires.
		/// </summary>
		public Action<TInstance, TSource, TEventArgs> OnEventAction { get; set; }

		/// <summary>
		/// Gets or sets the method to call when detaching from the event.
		/// </summary>
		public Action<WeakEventListener<TInstance, TSource, TEventArgs>> OnDetachAction { get; set; }

		/// <summary>
		/// Initializes a new instances of the WeakEventListener class.
		/// </summary>
		/// <param name="instance">Instance subscribing to the event.</param>
		public WeakEventListener(TInstance instance)
		{
			if (instance == null)
				throw new ArgumentNullException("instance");

			_weakInstance = new WeakReference(instance);
		}

		/// <summary>
		/// Handler for the subscribed event calls OnEventAction to handle it.
		/// </summary>
		/// <param name="source">Event source.</param>
		/// <param name="eventArgs">Event arguments.</param>
		public void OnEvent(TSource source, TEventArgs eventArgs)
		{
			var target = _weakInstance.Target as TInstance;

			if (target != null)
			{
				// Call the registered action.
				if (OnEventAction != null)
				{
					OnEventAction(target, source, eventArgs);
				}
			}
			else
			{
				// Detach from the event.
				Detach();
			}
		}

		/// <summary>
		/// Detaches from the subscribed event.
		/// </summary>
		public void Detach()
		{
			if (OnDetachAction != null)
			{
				OnDetachAction(this);
				OnDetachAction = null;
			}
		}
	}

	public interface IWeakEventListener
	{
		void Detach();
	}
}