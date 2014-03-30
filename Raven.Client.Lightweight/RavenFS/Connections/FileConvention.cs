using System;
using Raven.Client.Document;

namespace Raven.Client.RavenFS.Connections
{
	/// <summary>
	/// The set of conventions used by the <see cref="FileConvention"/> which allow the users to customize
	/// the way the Raven client API behaves
	/// </summary>
	public class FileConvention : Convention
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="FileConvention"/> class.
		/// </summary>
		public FileConvention()
		{
			MaxFailoverCheckPeriod = TimeSpan.FromMinutes(5);
			FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
			AllowMultipuleAsyncOperations = true;
			IdentityPartsSeparator = "/";
			ShouldCacheRequest = url => true;
		}

		/// <summary>
		/// Clone the current conventions to a new instance
		/// </summary>
		public FileConvention Clone()
		{
			return (FileConvention)MemberwiseClone();
		}
	}
}
