using System;
using System.Collections.Generic;
using System.Text;
using Jint.Native;
using Jint.Expressions;
using System.Reflection;
using System.Reflection.Emit;

namespace Jint.Marshal
{
    class JsFunctionDelegate
    {
        Delegate m_impl;
        IJintVisitor m_visitor;
        JsFunction m_function;
        JsDictionaryObject m_that;
        Marshaller m_marshaller;
        Type m_delegateType;

        public JsFunctionDelegate(IJintVisitor visitor, JsFunction function, JsDictionaryObject that,Type delegateType)
        {
            if (visitor == null)
                throw new ArgumentNullException("visitor");
            if (function == null)
                throw new ArgumentNullException("function");
            if (delegateType == null)
                throw new ArgumentNullException("delegateType");
            if (!typeof(Delegate).IsAssignableFrom(delegateType))
                throw new ArgumentException("A delegate type is required", "delegateType");
            m_visitor = visitor;
            m_function = function;
            m_delegateType = delegateType;
            m_that = that;
            m_marshaller = visitor.Global.Marshaller;
        }

        public Delegate GetDelegate()
        {
            if (m_impl!= null)
                return m_impl;

            MethodInfo method = m_delegateType.GetMethod("Invoke");
            ParameterInfo[] parameters = method.GetParameters();
            Type[] delegateParameters = new Type[parameters.Length + 1];
            
            for (int i = 1; i <= parameters.Length; i++)
                delegateParameters[i] = parameters[i - 1].ParameterType;
            delegateParameters[0] = typeof(JsFunctionDelegate);

            DynamicMethod dm = new DynamicMethod(
                "DelegateWrapper",
                method.ReturnType,
                delegateParameters,
                typeof(JsFunctionDelegate)
            );

            ILGenerator code = dm.GetILGenerator();

            // arg_0 - this
            // arg_1 ... arg_n - delegate parameters
            // local_0 parameters
            // local_1 marshaller

            code.DeclareLocal(typeof(JsInstance[]));
            code.DeclareLocal(typeof(Marshaller));

            // parameters = new JsInstance[...];
            code.Emit(OpCodes.Ldc_I4, parameters.Length);
            code.Emit(OpCodes.Newarr, typeof(JsInstance));
            code.Emit(OpCodes.Stloc_0);

            // load a marshller
            code.Emit(OpCodes.Ldarg_0);
            code.Emit(OpCodes.Ldfld,typeof(JsFunctionDelegate).GetField("m_marshaller",BindingFlags.NonPublic|BindingFlags.Instance));
            code.Emit(OpCodes.Stloc_1);

            //code.EmitWriteLine("pre args");

            for (int i = 1; i <= parameters.Length; i++)
            {
                ParameterInfo param = parameters[i-1];
                Type paramType = param.ParameterType;

                code.Emit(OpCodes.Ldloc_0);
                code.Emit(OpCodes.Ldc_I4, i - 1);

                // marshal arg
                code.Emit(OpCodes.Ldloc_1);
                code.Emit(OpCodes.Ldarg, i);
                
                // if parameter is passed by reference
                if (paramType.IsByRef)
                {
                    paramType = paramType.GetElementType();

                    if (param.IsOut && !param.IsIn)
                    {
                        code.Emit(OpCodes.Ldarg, i);
                        code.Emit(OpCodes.Initobj);
                    }

                    if (paramType.IsValueType)
                        code.Emit(OpCodes.Ldobj, paramType);
                    else
                        code.Emit(OpCodes.Ldind_Ref);
                }

                code.Emit(
                    OpCodes.Call,
                    typeof(Marshaller)
                        .GetMethod("MarshalClrValue")
                        .MakeGenericMethod(paramType)
                );
                // save arg

                code.Emit(OpCodes.Stelem, typeof(JsInstance) );
            }

            // m_visitor.ExecuteFunction(m_function,m_that,arguments)

            code.Emit(OpCodes.Ldarg_0);
            code.Emit(OpCodes.Ldfld, typeof(JsFunctionDelegate).GetField("m_visitor", BindingFlags.NonPublic | BindingFlags.Instance));

            code.Emit(OpCodes.Ldarg_0);
            code.Emit(OpCodes.Ldfld, typeof(JsFunctionDelegate).GetField("m_function", BindingFlags.NonPublic | BindingFlags.Instance));

            code.Emit(OpCodes.Ldarg_0);
            code.Emit(OpCodes.Ldfld, typeof(JsFunctionDelegate).GetField("m_that", BindingFlags.NonPublic | BindingFlags.Instance));

            code.Emit(OpCodes.Ldloc_0); //params

            code.Emit(OpCodes.Callvirt, typeof(IJintVisitor).GetMethod("ExecuteFunction"));


            // foreach out parameter, marshal it back
            for (int i = 1; i <= parameters.Length; i++)
            {
                ParameterInfo param = parameters[i-1];
                Type paramType = param.ParameterType.GetElementType();
                if (param.IsOut)
                {
                    code.Emit(OpCodes.Ldarg, i);

                    code.Emit(OpCodes.Ldloc_1);

                    code.Emit(OpCodes.Ldloc_0);
                    code.Emit(OpCodes.Ldc_I4, i - 1);
                    code.Emit(OpCodes.Ldelem, typeof(JsInstance));

                    code.Emit(OpCodes.Call, typeof(Marshaller).GetMethod("MarshalJsValue").MakeGenericMethod(paramType));

                    if (paramType.IsValueType)
                        code.Emit(OpCodes.Stobj, paramType);
                    else
                        code.Emit(OpCodes.Stind_Ref);
                }
            }

            // return marshaller.MarshalJsValue<method.ReturnType>(m_visitor.Returned)
            if (!method.ReturnType.Equals(typeof(void)))
            {
                code.Emit(OpCodes.Ldloc_1);
                code.Emit(OpCodes.Ldarg_0);
                code.Emit(OpCodes.Ldfld, typeof(JsFunctionDelegate).GetField("m_visitor", BindingFlags.NonPublic | BindingFlags.Instance));
                code.Emit(OpCodes.Call, typeof(IJintVisitor).GetProperty("Returned").GetGetMethod());
                code.Emit(OpCodes.Call, typeof(Marshaller).GetMethod("MarshalJsValue").MakeGenericMethod(method.ReturnType));
            }
            
            code.Emit(OpCodes.Ret);

            return m_impl = dm.CreateDelegate(m_delegateType,this);
        }
    }
}
