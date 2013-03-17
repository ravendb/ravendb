using System;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Extensions;
#if NETFX_CORE
using Raven.Imports.Newtonsoft.Json.Utilities;
#endif

namespace Raven.Abstractions.Logging.LogProviders
{
	public abstract class LogManagerBase : ILogManager
	{
		private readonly Func<object, ILog> loggerFactory;
		private readonly Func<string, object> getLoggerByNameDelegate;
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

		public Func<string, object> GetLoggerByNameDelegate
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
			return loggerFactory(getLoggerByNameDelegate(name));
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

		private Func<string, object> GetGetLoggerMethodCall()
		{
			Type logManagerType = GetLogManagerType();
			MethodInfo method = logManagerType.GetMethod("GetLogger", new[] {typeof (string)});
			ParameterExpression resultValue;
			ParameterExpression keyParam = Expression.Parameter(typeof (string), "key");
			MethodCallExpression methodCall = Expression.Call(null, method, new Expression[] {resultValue = keyParam});
			return Expression.Lambda<Func<string, object>>(methodCall, new[] {resultValue}).Compile();
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