//-----------------------------------------------------------------------
// <copyright file="AbstractDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractDeleteTrigger : IRequiresDocumentDatabaseInitialization
	{
		/// <summary>
		///  Ask the trigger whatever the DELETE should be vetoed.
		///  If the trigger vote to veto the DELETE, it needs to provide a human readable 
		///  explanation why the DELETE was rejected.
		///  </summary><remarks>
		///  This method SHOULD NOT modify either the document or the metadata.
		///  </remarks>
		/// <param name="key">The document key</param>
		/// <param name="transactionInformation">The current transaction, if any</param>
		/// <returns>Whatever the delete was vetoed or not</returns>
		public virtual VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
		{
			return VetoResult.Allowed;
		}

		/// <summary>
		///  Allow the trigger to perform any logic just before the document is deleted.
		///  </summary><remarks>
		///  If the trigger need to access the previous state of the document, the trigger should
		///  implement <seealso cref="IRequiresDocumentDatabaseInitialization" /> and use the provided
		///  <seealso cref="DocumentDatabase" /> instance to Get it. The returned result would be the old
		///  document (if it exists) or null.
		///  Any call to the provided <seealso cref="DocumentDatabase" /> instance will be done under the
		///  same transaction as the DELETE operation.
		///  </remarks><param name="transactionInformation">The current transaction, if any</param><param name="key">The document key</param>
		public virtual void OnDelete(string key, TransactionInformation transactionInformation)
		{
			
		}

		/// <summary>
		///  Allow the trigger to perform any logic after the document was deleted but still in the 
		///  same transaction as the delete.
		///  This method is called only if a row was actually deleted
		///  </summary><remarks>
		///  Any call to the provided <seealso cref="DocumentDatabase" /> instance will be done under the
		///  same transaction as the DELETE operation.
		///  </remarks><param name="transactionInformation">The current transaction, if any</param><param name="key">The document key</param>
		public virtual void AfterDelete(string key, TransactionInformation transactionInformation)
		{
			
		}

		/// <summary>
		///  Allow the trigger to perform any logic _after_ the transaction was committed.
		///  For example, by notifying interested parties.
		///  </summary><param name="key">The document key</param>
		public virtual void AfterCommit(string key)
		{
			
		}

		public void Initialize(DocumentDatabase database)
		{
			Database = database;
			Initialize();
		}

		public virtual void Initialize()
		{
			
		}

		public DocumentDatabase Database { get; set; }
	}
}
