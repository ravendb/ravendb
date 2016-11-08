using System;

using Raven.Json.Linq;

namespace Raven.Client.Documents
{
    public interface ILoadConfiguration
    {
        /// <summary>
        /// Adds transformer parameter that will be passed to transformer on server-side.
        /// </summary>
        /// <param name="name">name of the parameter</param>
        /// <param name="value">value of the parameter</param>
        [Obsolete("Use AddTransformerParameter instead.")]
        void AddQueryParam(string name, object value);

        /// <summary>
        /// Adds transformer parameter that will be passed to transformer on server-side.
        /// </summary>
        /// <param name="name">name of the parameter</param>
        /// <param name="value">value of the parameter</param>
        void AddTransformerParameter(string name, object value);

        void AddTransformerParameter(string name, DateTime value);
    }
}
