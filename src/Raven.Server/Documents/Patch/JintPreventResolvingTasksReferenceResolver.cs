using System;
using System.Threading.Tasks;
using Jint;
using Jint.Native;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Jint.Runtime.References;

namespace Raven.Server.Documents.Patch
{
    public class JintPreventResolvingTasksReferenceResolver : IReferenceResolver
    {
        public bool TryUnresolvableReference(Engine engine, Reference reference, out JsValue value)
        {
            value = Null.Instance;
            return true;
        }

        public bool TryPropertyReference(Engine engine, Reference reference, ref JsValue value)
        {
            if (value.IsObject() &&
                value.AsObject() is ObjectWrapper objectWrapper &&
                objectWrapper.Target is Task task &&
                reference.GetReferencedName() == nameof(Task<int>.Result) &&
                task.IsCompleted == false)
            {
                var descriptor = GetRunningTaskResult(task);
                value = descriptor.Value;
                return true;
            }

            return false;
        }

        public static PropertyDescriptor GetRunningTaskResult(Task task)
        {
            var value = $"{{Ignoring Task.Result as task's status is {task.Status.ToString()}}}.";
            if (task.IsFaulted)
                value += Environment.NewLine + "Exception: " + task.Exception;
            var jsValue = new JsValue(value);
            var descriptor = new PropertyDescriptor(jsValue, false, false, false);
            return descriptor;
        }

        public bool TryGetCallable(Engine engine, object callee, out JsValue value)
        {
            value = new JsValue(new ClrFunctionInstance(engine, (thisObj, values) => thisObj));
            return true;
        }

        public bool CheckCoercible(JsValue value)
        {
            return true;
        }
    }
}
