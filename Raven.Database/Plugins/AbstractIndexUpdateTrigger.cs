//-----------------------------------------------------------------------
// <copyright file="AbstractIndexUpdateTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
	/// <summary>
	/// Implementers of this class are called whenever an index entry is 
	/// created / deleted.
	/// Work shouldn't be done by the methods of this interface, rather, they
	/// should be done in a background thread. Communication between threads can
	/// use either in memory data structure or the persistent (and transactional )
	/// queue implementation available on the transactional storage.
	/// </summary>
	/// <remarks>
	/// * All operations are delete/create operations, whatever the value
	///   previously existed or not.
	/// * It is possible for OnIndexEntryDeleted to be called for non existent
	///   values.
	/// * It is possible for a single entry key to be called inserted several times
	///   entry keys are NOT unique.
	/// </remarks>
	[InheritedExport]
	public abstract class AbstractIndexUpdateTrigger : IRequiresDocumentDatabaseInitialization
	{
		public void Initialize(DocumentDatabase database)
		{
			Database = database;
			Initialize();
		}

		public virtual void Initialize()
		{
			
		}

		public virtual void SecondStageInit()
		{

		}


		public abstract AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName);

		public DocumentDatabase Database { get; set; }
	}
}
