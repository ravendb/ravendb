using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;
using V8.Net;


namespace Raven.Server.Documents.Patch.V8
{

    public class TaskCustomBinder : ObjectBinderEx<Task>
    {
        public static InternalHandle CreateObjectBinder(V8EngineEx engine, Task oi, bool keepAlive = false) 
        {
            var jsBinder = engine.Engine.CreateObjectBinder<TaskCustomBinder>(oi, engine.Context.TypeBinderTask(), keepAlive: keepAlive);
            var binder = (ObjectBinder)jsBinder.Object;
            binder.ShouldDisposeBoundObject = true;
            return jsBinder;
        }

        public static InternalHandle GetRunningTaskResult(V8Engine engine, Task task)
        {
            try
            {
                var value = $"{{Ignoring Task.Result as task's status is {task.Status.ToString()}}}.";
                if (task.IsFaulted)
                    value += Environment.NewLine + "Exception: " + task.Exception;
                return engine.CreateValue(value);
            }
            catch (Exception e)
            {
                //TODO: egor
          //      engine.Context.JsContext.LastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        public override InternalHandle NamedPropertyGetter(ref string propertyName)
        {
            try
            {
                if (ObjClr.IsCompleted == false && propertyName == nameof(Task<int>.Result))
                {
                    return GetRunningTaskResult(Engine, ObjClr);
                }
                return base.NamedPropertyGetter(ref propertyName);
            }
            catch (Exception e)
            {
               // engineEx.Context.JsContext.LastException = e;
                return Engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

    }
}
