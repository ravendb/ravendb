using System;

namespace Raven.Database
{
	/// <summary>
	/// Transaction information that identify the transaction id and timeout
	/// </summary>
    public class TransactionInformation
    {
		/// <summary>
		/// Gets or sets the id.
		/// </summary>
		/// <value>The id.</value>
        public Guid Id { get; set; }
		/// <summary>
		/// Gets or sets the timeout.
		/// </summary>
		/// <value>The timeout.</value>
        public TimeSpan Timeout { get; set; }
    }
}
