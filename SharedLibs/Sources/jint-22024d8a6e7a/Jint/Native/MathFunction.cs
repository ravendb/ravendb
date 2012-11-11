using System;
using Jint.Expressions;

namespace Jint.Native
{
	public class MathFunction : JsFunction
	{
		private Type[] types;
		Delegate impl;
		private int argLen;

		public MathFunction(Func<JsInstance> func, JsObject prototype)
			: base(prototype)
		{
			this.impl = func;
			this.argLen = 0;
		}

		public MathFunction(Func<double, JsInstance> func, JsObject prototype)
			: base(prototype)
		{
			this.impl = func;
			this.argLen = 1;
		}

		public MathFunction(Func<double, double, JsInstance> func, JsObject prototype)
			: base(prototype)
		{
			this.impl = func;
			this.argLen = 2;
		}

		public override JsInstance Execute(IJintVisitor visitor, JsDictionaryObject that, JsInstance[] parameters)
		{
			try
			{
				JsInstance result;

				switch (argLen)
				{
					case 0:
						result = impl.DynamicInvoke() as JsInstance;
						break;
					case 1:
						result = impl.DynamicInvoke(parameters[0].Value) as JsInstance;
						break;
					case 2:
						result = impl.DynamicInvoke(parameters[0].Value, parameters[1].Value) as JsInstance;
						break;
					default:
						throw new ArgumentOutOfRangeException("argLen");
				}
				
				visitor.Return(result);
				return result;
			}
			catch (ArgumentException)
			{
				var constructor = that["constructor"] as JsFunction;
				throw new JsException(visitor.Global.TypeErrorClass.New("incompatible type: " + constructor == null ? "<unknown>" : constructor.Name));
			}
			catch (Exception e)
			{
				if (e.InnerException is JsException)
				{
					throw e.InnerException;
				}

				throw;
			}
		}

		public override string ToString()
		{
			return String.Format("function {0}() { [native code] }", impl.Method.Name);
		}
	}
}