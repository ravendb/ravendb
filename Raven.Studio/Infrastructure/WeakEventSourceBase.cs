using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Raven.Studio.Infrastructure
{
	/// <summary>
	/// Base class to wrap a specific event with the WeakEventListener.
	/// </summary>
	/// <typeparam name="TSource">The type of the event source.</typeparam>
	public abstract class WeakEventSourceBase<TSource>
		where TSource : class
	{
		/// <summary>
		/// A weak reference to the event source
		/// </summary>
		private WeakReference weakEventSource;

		/// <summary>
		///  A weak reference to the WeakEventListener instance.
		/// </summary>
		private WeakReference weakListener;

		/// <summary>
		/// Gets the event source instance which this listener is using.
		/// </summary>
		/// <remarks>
		/// The reference to the event source is weak.
		/// </remarks>
		public object EventSource
		{
			get
			{
				if (weakEventSource == null)
					return null;
				return weakEventSource.Target;
			}
		}

		/// <summary>
		/// Set the event source for this instance. 
		/// When passing a new event source it replaces the event source the 
		/// listener is listen for an event. When passing null/nothing is detaches 
		/// the previous event source from this event listener. 
		/// </summary>
		/// <param name="eventSource">The event source instance.</param>
		public void SetEventSource(object eventSource)
		{
			// the listener can just listen for one event source. 
			// Detach the previous event source
			Detach();

			// keep weak-reference to the the event source
			weakEventSource = new WeakReference(eventSource);

			var eventObject = eventSource as TSource;
			if (eventObject != null)
			{
				var weakListener = CreateWeakEventListenerInternal(eventObject);

				if (weakListener == null)
					throw new InvalidOperationException("The method CreateWeakEventListener must return a value.");

				// store the weak-listener as weak reference (for Detach method only)
				this.weakListener = new WeakReference(weakListener);
			}
		}

		/// <summary>
		/// Does some debug-time checks and creates the weak event listener.
		/// </summary>
		/// <param name="eventObject">The event source instance</param>
		/// <returns>Return the weak event listener instance</returns>
		private IWeakEventListener CreateWeakEventListenerInternal(TSource eventObject)
		{
			#region Debug time checks

			// do some implementation checks when a debugger is attached
			if (Debugger.IsAttached)
			{
				// search in each type separately until we reach the type WeakEventSourceBase 
				//(because Reflection can not return private members in FlattenHierarchy.
				Type type = this.GetType();
				while ((!type.IsGenericType || type.GetGenericTypeDefinition() != typeof (WeakEventSourceBase<>)) && type != typeof (object))
				{
					BindingFlags bindingFlags = BindingFlags.Public |
					                            BindingFlags.NonPublic |
					                            BindingFlags.Instance |
					                            BindingFlags.Static |
					                            BindingFlags.DeclaredOnly;

					// get fields expect fields marked with CompilerGeneratedAttribute or derived from Delegate (events are delegate fields)
					var queryFields = from f in type.GetFields(bindingFlags)
					                  where f.GetCustomAttributes(typeof (CompilerGeneratedAttribute), true).Count() == 0 &&
					                        !f.FieldType.IsSubclassOf(typeof (Delegate))
					                  select f.Name;

					// get properties
					var queryProperties = from f in type.GetProperties(bindingFlags)
					                      select f.Name;

					var query = queryFields.Union(queryProperties);

					// The EventWrapper is intended to be used as a weak-event-wrapper. One should not add additional 
					// members to this class, because of the possibility to store the WeakEventListener reference to a member.
					// Is this the case the memory leak can still occur. 
					// Therefore, if any field or property is implemented, throw an exception as warning.
					if (query.Count() > 0)
					{
						// note: MessageBox.Show blocks unit tests
						throw new InvalidOperationException(string.Format("You should not add any other implementation than overriding methods in the class {0}, because of possible memory you can get within your application.", type.Name));
					}

					// continue search in base type
					type = type.BaseType;
				}
			}

			#endregion

			// create weak event listener
			return CreateWeakEventListener(eventObject);
		}

		/// <summary>
		/// When overridden in a derived class, it creates the weak event listener for the given event source.
		/// </summary>
		/// <param name="eventObject">The event source instance to listen for an event</param>
		/// <returns>Return the weak event listener instance</returns>
		protected abstract IWeakEventListener CreateWeakEventListener(TSource eventObject);

		/// <summary>
		/// Detaches the event from the event source.
		/// </summary>
		public void Detach()
		{
			if (weakListener != null)
			{
				// do it the GC safe way, because an object could potentially be reclaimed 
				// for garbage collection immediately after the IsAlive property returns true
				var target = weakListener.Target as IWeakEventListener;
				if (target != null)
					target.Detach();
			}

			weakEventSource = null;
			weakListener = null;
		}
	}
}