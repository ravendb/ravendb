using System;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Logging.LogProviders
{
    public abstract class LogManagerBase : ILogManager
    {
        private readonly Func<object, ILog> loggerFactory;
        private readonly Func<Assembly, string, object> getLoggerByNameDelegate;
        private readonly Action<string> mdcRemoveMethodCall;
        private readonly Action<string, string> mdcSetMethodCall;
        private readonly Func<string, IDisposable> ndcPushMethodCall;

        protected LogManagerBase(Func<object, ILog> loggerFactory)
        {
            this.loggerFactory = loggerFactory;
            getLoggerByNameDelegate = GetGetLoggerMethodCall();
            ndcPushMethodCall = GetNdcPushMethodCall();
            mdcSetMethodCall = GetMdcSetMethodCall();
            mdcRemoveMethodCall = GetMdcRemoveMethodCall();
        }

        public Func<Assembly, string, object> GetLoggerByNameDelegate
        {
            get { return getLoggerByNameDelegate; }
        }
        public Func<string, IDisposable> NdcPushMethodCall
        {
            get { return ndcPushMethodCall; }
        }
        public Action<string, string> MdcSetMethodCall
        {
            get { return mdcSetMethodCall; }
        }
        public Action<string> MdcRemoveMethodCall
        {
            get { return mdcRemoveMethodCall; }
        }

        protected abstract Type GetLogManagerType();

        protected abstract Type GetNdcType();

        protected abstract Type GetMdcType();

        
        public ILog GetLogger(string name)
        {
            var @type = this.GetType();
            Assembly @asm;
#if !DNXCORE50
            @asm = @type.Assembly;
#else
            @asm = @type.GetTypeInfo().Assembly;
#endif
            
            return loggerFactory(getLoggerByNameDelegate(@asm, name));
        }

        public IDisposable OpenNestedConext(string message)
        {
            return ndcPushMethodCall(message);
        }

        public IDisposable OpenMappedContext(string key, string value)
        {
            mdcSetMethodCall(key, value);
            return new DisposableAction(() => mdcRemoveMethodCall(key));
        }

        private Func<Assembly, string, object> GetGetLoggerMethodCall()
        {
            Type logManagerType = GetLogManagerType();
            MethodInfo method;
            
            ParameterExpression assembly = Expression.Parameter(typeof(Assembly), "repositoryAssembly");
            ParameterExpression name = Expression.Parameter(typeof(string), "name");

            method = logManagerType.GetMethod("GetLogger", new[] { typeof(string) });
            if (method != null)
            {
                MethodCallExpression methodCall =
                    Expression.Call(null, method, new Expression[]
                    {
                        name
                    });

                var block = Expression.Block(methodCall);
                return Expression.Lambda<Func<Assembly, string, object>>(block, new[] { assembly, name}).Compile();
            }

            method = logManagerType.GetMethod("GetLogger", new[] { typeof(Assembly), typeof(string) });
            if (method != null)
            {
                MethodCallExpression methodCall =
                    Expression.Call(null, method, new Expression[]
                    {
                    assembly,
                    name
                    });
                return Expression.Lambda<Func<Assembly, string, object>>(methodCall, new[] { assembly, name }).Compile();
            }
            Debug.Assert(false, "Could not find a valid logger"); // we'll throw only if in debug, for release we'll keep running without logs
            var returnNullBlock = Expression.Block(Expression.Label(Expression.Label(typeof(object)),Expression.Constant(null)));
            return Expression.Lambda<Func<Assembly, string, object>>(returnNullBlock, new[] { assembly, name }).Compile();
            
        }

        private Func<string, IDisposable> GetNdcPushMethodCall()
        {
            Type ndcType = GetNdcType();
            MethodInfo method = ndcType.GetMethod("Push", new[] {typeof (string)});
            ParameterExpression resultValue;
            ParameterExpression keyParam = Expression.Parameter(typeof (string), "key");
            MethodCallExpression methodCall = Expression.Call(null, method, new Expression[] {resultValue = keyParam});
            return Expression.Lambda<Func<string, IDisposable>>(methodCall, new[] {resultValue}).Compile();
        }

        private Action<string, string> GetMdcSetMethodCall()
        {
            Type mdcType = GetMdcType();
            MethodInfo method = mdcType.GetMethod("Set", new[] {typeof (string), typeof (string)});
            ParameterExpression keyParam = Expression.Parameter(typeof (string), "key");
            ParameterExpression valueParam = Expression.Parameter(typeof (string), "value");
            MethodCallExpression methodCall = Expression.Call(null, method, new Expression[] {keyParam, valueParam});
            return Expression.Lambda<Action<string, string>>(methodCall, new[] {keyParam, valueParam}).Compile();
        }

        private Action<string> GetMdcRemoveMethodCall()
        {
            Type mdcType = GetMdcType();
            MethodInfo method = mdcType.GetMethod("Remove", new[] {typeof (string)});
            ParameterExpression keyParam = Expression.Parameter(typeof (string), "key");
            MethodCallExpression methodCall = Expression.Call(null, method, new Expression[] {keyParam});
            return Expression.Lambda<Action<string>>(methodCall, new[] {keyParam}).Compile();
        }
    }
}
