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

        public object this[string name]
        {
            set => ((BlittableObjectInstance)_instance)[name] = value;
        }

        public ObjectInstance Get(string name)
        {
            if (_instance is ObjectInstance parent)
            {
                var o = parent[name] as ObjectInstance;
                if (o == null)
                {
                    parent[name] = o = parent.Engine.Object.Construct();
                }
                return o;
            }
            ThrowInvalidObject(name);
            return null; // never hit
        }

        private void ThrowInvalidObject(string name) => 
            throw new InvalidOperationException("Unable to get property '" + name + "' because the result is not an object but: " + _instance);

        public object Value => _instance;

        public T Translate<T>(JsonOperationContext context,
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
