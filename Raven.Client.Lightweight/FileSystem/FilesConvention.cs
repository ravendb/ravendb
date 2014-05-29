using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;

namespace Raven.Client.FileSystem
{
    /// <summary>
    /// The set of conventions used by the <see cref="FilesConvention"/> which allow the users to customize
    /// the way the Raven client API behaves
    /// </summary>
    public class FilesConvention : Convention
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FilesConvention"/> class.
		/// </summary>
        public FilesConvention()
		{
			FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
			AllowMultipuleAsyncOperations = true;
			IdentityPartsSeparator = "/";
			ShouldCacheRequest = url => true;
		}

		/// <summary>
		/// Clone the current conventions to a new instance
		/// </summary>
        public FilesConvention Clone()
		{
            return (FilesConvention)MemberwiseClone();
		}
    }
}
