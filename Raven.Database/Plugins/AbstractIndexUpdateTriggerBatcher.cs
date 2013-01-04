//-----------------------------------------------------------------------
// <copyright file="AbstractIndexUpdateTriggerBatcher.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Lucene.Net.Documents;

namespace Raven.Database.Plugins
{
	public abstract class AbstractIndexUpdateTriggerBatcher : IDisposable
	{
		/// <summary>
		///  Notify that a document with the specified key was deleted
		///  Key may represent a missing document
		///  </summary>
		/// <param name="entryKey">The entry key</param>
		public virtual void OnIndexEntryDeleted(string entryKey) { }

		/// <summary>
		///  Notify that the specified document with the specified key is about 
		///  to be inserted.
		///  </summary><remarks>
		///  You may modify the provided lucene document, changes made to the document
		///  will be written to the Lucene index
		///  </remarks>
		/// <param name="entryKey">The entry key</param><param name="document">The lucene document about to be written</param>
		public virtual void OnIndexEntryCreated(string entryKey, Document document) { }

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
