using System;
using System.Threading.Tasks;
using Jint;
using Jint.Native;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Jint.Runtime.References;

namespace Raven.Server.Documents.Patch
{
    public class JintPreventResolvingTasksReferenceResolver : JintNullPropagationReferenceResolver
    {
        public void ExplodeArgsOn(JsValue self, BlittableObjectInstance args)
        {
            _selfInstance = self;
            _args = args;
        }

        public override bool TryPropertyReference(Engine engine, Reference reference, ref JsValue value)
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

            return base.TryPropertyReference(engine, reference, ref value);
        }

        public static PropertyDescriptor GetRunningTaskResult(Task task)
        {
            var value = $"{{Ignoring Task.Result as task's status is {task.Status.ToString()}}}.";
            if (task.IsFaulted)
                value += Environment.NewLine + "Exception: " + task.Exception;
            var jsValue = value;
            var descriptor = new PropertyDescriptor(jsValue, false, false, false);
            return descriptor;
        }
    }
}
