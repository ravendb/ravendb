using System;
using System.Threading.Tasks;
using Jint;
using Jint.Native;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Jint.Runtime.References;

namespace Raven.Server.Documents.Patch
{
    public class JintPreventResolvingTasksReferenceResolver : JintNullPropgationReferenceResolver
    {
        private JsValue _selfInstance;
        private BlittableObjectInstance _args;

        public void ExplodeArgsOn(JsValue self, BlittableObjectInstance args)
        {
            _selfInstance = self;
            _args = args;
        }

        public override bool TryUnresolvableReference(Engine engine, Reference reference, out JsValue value)
        {
            if(engine.ExecutionContext.ThisBinding == _selfInstance)
            {
                var name = reference.GetReferencedName();
                if(name == null || name.StartsWith('$') == false)
                    return base.TryUnresolvableReference(engine, reference, out value);

                name = name.Substring(1);
                value = _args.Get(name);
                return true;
            }
            return base.TryUnresolvableReference(engine, reference, out value);
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
            var jsValue = new JsValue(value);
            var descriptor = new PropertyDescriptor(jsValue, false, false, false);
            return descriptor;
        }
    }
}
