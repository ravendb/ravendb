using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Jint;
using Jint.Parser;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json.Linq;

namespace Raven.Server.Documents.Patch
{
    [CLSCompliant(false)]
    public class ScriptsCache
    {
        private class CachedResult
        {
            public int Usage;
            public DateTime Timestamp;
            public ConcurrentQueue<Engine> Queue;
        }

        private const int CacheMaxSize = 512;

        private readonly ConcurrentDictionary<ScriptedPatchRequestAndCustomFunctionsToken, CachedResult> _cache =
            new ConcurrentDictionary<ScriptedPatchRequestAndCustomFunctionsToken, CachedResult>();

        private class ScriptedPatchRequestAndCustomFunctionsToken
        {
            private readonly ScriptedPatchRequest request;
            private readonly string customFunctions;

            public ScriptedPatchRequestAndCustomFunctionsToken(ScriptedPatchRequest request, string customFunctions)
            {
                this.request = request;
                this.customFunctions = customFunctions;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(this, obj)) return true;
                var other = obj as ScriptedPatchRequestAndCustomFunctionsToken;
                if (ReferenceEquals(null, other)) return false;
                if (request.Equals(other.request))
                {
                    if (customFunctions == null && other.customFunctions == null)
                        return true;
                    if (customFunctions != null && other.customFunctions != null)
                        return RavenJTokenEqualityComparer.Default.Equals(customFunctions, other.customFunctions);
                }
                return false;
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((request != null ? request.GetHashCode() : 0) * 397) ^
                        (customFunctions != null ? RavenJTokenEqualityComparer.Default.GetHashCode(customFunctions) : 0);
                }
            }
        }

        public void CheckinScript(ScriptedPatchRequest request, Engine context, string customFunctions)
        {
            CachedResult cacheByCustomFunctions;

            var patchRequestAndCustomFunctionsTuple = new ScriptedPatchRequestAndCustomFunctionsToken(request, customFunctions);
            if (_cache.TryGetValue(patchRequestAndCustomFunctionsTuple, out cacheByCustomFunctions))
            {
                if (cacheByCustomFunctions.Queue.Count > 20)
                    return;
                cacheByCustomFunctions.Queue.Enqueue(context);
                return;
            }
            _cache.AddOrUpdate(patchRequestAndCustomFunctionsTuple, patchRequest =>
            {
                var queue = new ConcurrentQueue<Engine>();

                return new CachedResult
                {
                    Queue = queue,
                    Timestamp = SystemTime.UtcNow,
                    Usage = 1
                };
            }, (patchRequest, result) =>
            {
                result.Queue.Enqueue(context);
                return result;
            });
        }

        public Engine CheckoutScript(Func<ScriptedPatchRequest, Engine> createEngine, ScriptedPatchRequest request, string customFunctions)
        {
            CachedResult value;
            var patchRequestAndCustomFunctionsTuple = new ScriptedPatchRequestAndCustomFunctionsToken(request, customFunctions);
            if (_cache.TryGetValue(patchRequestAndCustomFunctionsTuple, out value))
            {
                Interlocked.Increment(ref value.Usage);
                Engine context;
                if (value.Queue.TryDequeue(out context))
                {
                    return context;
                }
            }
            var result = createEngine(request);

            if (string.IsNullOrWhiteSpace(customFunctions) == false)

                result.Execute(string.Format(@"var customFunctions = function() {{  var exports = {{ }}; {0};
                            return exports;
                        }}();
                        for(var customFunction in customFunctions) {{
                            this[customFunction] = customFunctions[customFunction];
                        }};", customFunctions), new ParserOptions { Source = "customFunctions.js" });
            var cachedResult = new CachedResult
            {
                Usage = 1,
                Queue = new ConcurrentQueue<Engine>(),
                Timestamp = SystemTime.UtcNow
            };

            _cache.AddOrUpdate(patchRequestAndCustomFunctionsTuple, cachedResult, (_, existing) =>
            {
                Interlocked.Increment(ref existing.Usage);
                return existing;
            });
            if (_cache.Count > CacheMaxSize)
            {
                foreach (var source in _cache
                    .Where(x => x.Value != null)
                    .OrderByDescending(x => x.Value.Usage)
                    .ThenBy(x => x.Value.Timestamp)
                    .Skip(CacheMaxSize - CacheMaxSize / 10))
                {
                    if (Equals(source.Key, request))
                        continue; // we don't want to remove the one we just added
                    CachedResult ignored;
                    _cache.TryRemove(source.Key, out ignored);
                }
                foreach (var source in _cache.Where(x => x.Value == null))
                {
                    CachedResult ignored;
                    _cache.TryRemove(source.Key, out ignored);
                }
            }

            return result;
        }
    }
}
