//-----------------------------------------------------------------------
// <copyright file="AbstractReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Plugins
{
	/// <summary>
	/// * Read triggers may be called on projections from indexes, not just documents
	/// </summary>
	[InheritedExport]
	public abstract class AbstractReadTrigger : IRequiresDocumentDatabaseInitialization
	{
		/// <summary>
		///  Ask the trigger whatever the document should be read by the user.
		///  </summary><remarks>
		///  The document and metadata instances SHOULD NOT be modified.
		///  </remarks>
		/// <param name="key">The key of the read document - can be null if reading a projection</param>
		/// <param name="metadata">The document metadata</param>
		/// <param name="operation">Whatever the operation is a load or a query</param>
		/// <param name="transactionInformation">The transaction information, if any</param>
		/// <returns>
		///  * If the result is Allow, the operation continues as usual. 
		///  * If the result is Deny, the operation will return an error to the user 
		///    if asking for a particular document, or an error document in place of 
		///    the result if asking for a query.
		///  * If the result is Ignore, the operation will return null to the user if
		///    asking for a particular document, or skip including the result entirely 
		///    in the query results.
		///  </returns>
		public virtual ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
		{
			return ReadVetoResult.Allowed;
		}

		/// <summary>
		///  Allow the trigger the option of modifying the document and metadata instances
		///  before the user can see them. 
		///  </summary><remarks>
		///  The modified values are transient, and are NOT saved to the database.
		///  </remarks>
		/// <param name="key">The key of the read document - can be null if reading a projection</param>
		/// <param name="document">The document being read</param>
		/// <param name="metadata">The document metadata</param>
		/// <param name="operation">Whatever the operation is a load or a query</param>
		/// <param name="transactionInformation">The transaction information, if any</param>
		public virtual void OnRead(string key, RavenJObject document, RavenJObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
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

		public virtual void SecondStageInit()
		{

		}


		public DocumentDatabase Database { get; set; }
	}
}
