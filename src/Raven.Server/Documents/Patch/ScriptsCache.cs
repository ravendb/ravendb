using System;
using System.Collections.Generic;
using System.Linq;
using Jurassic;
using Raven.Client.Util;

namespace Raven.Server.Documents.Patch
{
    [CLSCompliant(false)]
    public class ScriptsCache
    {
        private class CachedResult
        {
            public int Usage;
            public DateTime Timestamp;
            public ScriptEngine Engine;
        }

        private const int CacheMaxSize = 512;

        private readonly Dictionary<ScriptedPatchRequestAndCustomFunctionsToken, CachedResult> _cache =
            new Dictionary<ScriptedPatchRequestAndCustomFunctionsToken, CachedResult>();

        private class ScriptedPatchRequestAndCustomFunctionsToken
        {
            private readonly PatchRequest _request;
            private readonly string _customFunctions;

            public ScriptedPatchRequestAndCustomFunctionsToken(PatchRequest request, string customFunctions)
            {
                _request = request;
                _customFunctions = customFunctions;
            }

            private bool Equals(ScriptedPatchRequestAndCustomFunctionsToken other)
            {
                return Equals(_request, other._request) && string.Equals(_customFunctions, other._customFunctions, StringComparison.OrdinalIgnoreCase);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((ScriptedPatchRequestAndCustomFunctionsToken)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((_request?.GetHashCode() ?? 0) * 397) ^ (_customFunctions != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(_customFunctions) : 0);
                }
            }
        }

        public ScriptEngine GetEngine(Func<PatchRequest, ScriptEngine> createEngine, PatchRequest request, string customFunctions)
        {
            var patchRequestAndCustomFunctionsTuple = new ScriptedPatchRequestAndCustomFunctionsToken(request, customFunctions);
            if (_cache.TryGetValue(patchRequestAndCustomFunctionsTuple, out CachedResult value))
            {
                value.Usage++;
                return value.Engine;
            }
            var result = createEngine(request);

            if (string.IsNullOrWhiteSpace(customFunctions) == false)

                result.Execute(string.Format(@"var customFunctions = function() {{  var exports = {{ }}; {0};
                            return exports;
                        }}();
                        for(var customFunction in customFunctions) {{
                            this[customFunction] = customFunctions[customFunction];
                        }};", customFunctions));
            var cachedResult = new CachedResult
            {
                Usage = 1,
                Timestamp = SystemTime.UtcNow,
                Engine = result
            };

            if (_cache.Count > CacheMaxSize)
            {
                foreach (var item in _cache.OrderBy(x => x.Value?.Usage)
                    .ThenByDescending(x => x.Value?.Timestamp)
                    .Take(CacheMaxSize / 10)
                    .Select(source => source.Key)
                    .ToList())
                {
                    _cache.Remove(item);
                }
            }
            _cache[patchRequestAndCustomFunctionsTuple] = cachedResult;


            return result;
        }
    }
}
