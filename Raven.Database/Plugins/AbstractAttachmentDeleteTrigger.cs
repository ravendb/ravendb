//-----------------------------------------------------------------------
// <copyright file="AbstractAttachmentDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractAttachmentDeleteTrigger : IRequiresDocumentDatabaseInitialization
	{
		/// <summary>
		///  Ask the trigger whatever the DELETE should be vetoed.
		///  If the trigger vote to veto the DELETE, it needs to provide a human readable 
		///  explanation why the DELETE was rejected.
		///  </summary><remarks>
		///  This method SHOULD NOT modify either the attachment or the metadata.
		///  </remarks><param name="key">The attachment key</param>
		/// <returns>Whatever the put was vetoed or not</returns>
		public virtual VetoResult AllowDelete(string key)
		{
			return VetoResult.Allowed;
		}

		/// <summary>
		///  Allow the trigger to perform any logic just before the attachment is deleted.
		///  </summary><remarks>
		///  If the trigger need to access the previous state of the attachment, the trigger should
		///  implement <seealso cref="IRequiresDocumentDatabaseInitialization" /> and use the provided
		///  <seealso cref="DocumentDatabase" /> instance to Get it. The returned result would be the old
		///  document (if it exists) or null.
		///  Any call to the provided <seealso cref="DocumentDatabase" /> instance will be done under the
		///  same transaction as the DELETE operation.
		///  </remarks>
		/// <param name="key">The document key</param>
		public virtual void OnDelete(string key)
		{

		}

		/// <summary>
		///  Allow the trigger to perform any logic after the attachment was deleted but still in the 
		///  same transaction as the delete.
		///  This method is called only if a row was actually deleted
		///  </summary><remarks>
		///  Any call to the provided <seealso cref="DocumentDatabase" /> instance will be done under the
		///  same transaction as the DELETE operation.
		///  </remarks>
		/// <param name="key">The attachment key</param>
		public virtual void AfterDelete(string key)
		{

		}

		/// <summary>
		///  Allow the trigger to perform any logic _after_ the transaction was committed.
		///  For example, by notifying interested parties.
		///  </summary><param name="key">The attachment key</param>
		public virtual void AfterCommit(string key)
		{

		}

		public void Initialize(DocumentDatabase database)
		{
			Database = database;
			Initialize();
		}

		public virtual void SecondStageInit()
		{
			
		}

		public virtual void Initialize()
		{

		}

		public DocumentDatabase Database { get; set; }
	}
}