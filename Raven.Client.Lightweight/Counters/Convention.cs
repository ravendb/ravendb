using Raven.Abstractions.Replication;

namespace Raven.Client.Counters
{
	/// <summary>
	/// The set of conventions used by the <see cref="Convention"/> which allow the users to customize
	/// the way the Raven client API behaves
	/// </summary>
	public class Convention : Client.Convention
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="Convention"/> class.
		/// </summary>
		public Convention()
		{
			FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
			AllowMultipuleAsyncOperations = true;
			IdentityPartsSeparator = "/";
			ShouldCacheRequest = url => true;
		}

		/// <summary>
		/// Clone the current conventions to a new instance
		/// </summary>
		public Convention Clone()
		{
			return (Convention)MemberwiseClone();
		}
	}
}
