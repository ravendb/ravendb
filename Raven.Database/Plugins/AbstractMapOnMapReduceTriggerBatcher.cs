using System;
using Raven.Json.Linq;

namespace Raven.Database.Plugins
{
	public abstract class AbstractMapOnMapReduceTriggerBatcher : IDisposable
	{
		/// <summary>
		///  Notify that the specified object was mapped in a map reduce index.
		/// </summary>
		/// <remarks>
		///  Note that any new fields might not be picked up by the reduce step, if the reduce function on the index isn't aware of them. 
		/// </remarks>
		/// <param name="entryKey">The entry key</param>
		/// <param name="obj">The object that was just mapped </param>
		public virtual void OnObjectMapped(string entryKey, RavenJObject obj)
		{
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public virtual void Dispose()
		{

		}

		/// <summary>
		/// Notify the batcher that an error occured, and that it might want to NOT do any work during the Dispose phase.
		/// </summary>
		/// <param name="exception"></param>
		public virtual void AnErrorOccured(Exception exception)
		{

		}
	}
}