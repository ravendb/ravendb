using System;

using Raven.Json.Linq;

namespace Raven.Client
{
    public interface ILoadConfiguration
    {
		[Obsolete("Use AddTransformerParameter instead.")]
		void AddQueryParam(string name, RavenJToken value);

        void AddTransformerParameter(string name, RavenJToken value);
    }
}