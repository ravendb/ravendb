using System;

using Raven.Abstractions.Replication;
using Raven.Client.Document;

namespace Raven.Client.Counters.Connections
{
	/// <summary>
	/// The set of conventions used by the <see cref="CounterConvention"/> which allow the users to customize
	/// the way the Raven client API behaves
	/// </summary>
	public class CounterConvention : Convention
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="CounterConvention"/> class.
		/// </summary>
		public CounterConvention()
		{
			FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
			AllowMultipuleAsyncOperations = true;
			IdentityPartsSeparator = "/";
			ShouldCacheRequest = url => true;
		}

		/// <summary>
		/// Clone the current conventions to a new instance
		/// </summary>
		public CounterConvention Clone()
		{
			return (CounterConvention)MemberwiseClone();
		}
	}
}
