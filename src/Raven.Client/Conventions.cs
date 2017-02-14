using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace Raven.Client
{
    public abstract class Conventions
    {
        /// <summary>
        /// Enable multiple async operations
        /// </summary>
        public bool AllowMultipleAsyncOperations { get; set; }

        public double RequestTimeThresholdInMilliseconds { get; set; }

        public string AuthenticationScheme { get; set; }
    }
}
