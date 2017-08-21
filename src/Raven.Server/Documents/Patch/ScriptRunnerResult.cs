using System;
using Jurassic.Library;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunnerResult
    {
        private readonly object _instance;

        public ScriptRunnerResult(object instance)
        {
            _instance = instance;
        }

        public T TranslateFromJurrasic<T>(JsonOperationContext context, 
            BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            if (_instance == null)
                return default(T);

            if (_instance is ArrayInstance)
                ThrowInvalidArrayResult();
            if (typeof(T) == typeof(BlittableJsonReaderObject))
            {
                if (_instance is ObjectInstance obj)
                    return (T)(object)JurrasicBlittableBridge.Translate(context, obj, usageMode);
                ThrowInvalidObject();
            }
            return (T)_instance;
        }

        private void ThrowInvalidObject() => 
            throw new InvalidOperationException("Cannot translate instance to object because it is: " + _instance);

        private static void ThrowInvalidArrayResult() =>
            throw new InvalidOperationException("Script cannot return an array.");

    }
}
