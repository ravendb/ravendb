using System;

namespace Raven.NewClient.Client.Http
{
    public class RequestExecuterOptions
    {
        private readonly ConventionBase _conventions;
        private Func<string, bool> _shouldCacheRequest;

        public RequestExecuterOptions()
        {
        }

        public RequestExecuterOptions(ConventionBase conventions)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));

            _conventions = conventions;
        }

        /// <summary>
        /// Whatever or not request should be cached or not (request must contain Etag information for caching purposes)
        /// </summary>

        public Func<string, bool> ShouldCacheRequest
        {
            get
            {
                if (_conventions != null)
                    return _conventions.ShouldCacheRequest;

                return _shouldCacheRequest ?? (_shouldCacheRequest = s => true);
            }
            set
            {
                if (_conventions != null)
                {
                    _conventions.ShouldCacheRequest = value;
                    return;
                }

                _shouldCacheRequest = value;
            }
        }
    }
}