using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Esprima.Ast;
using Jint;
using Jint.Constraints;
using Jint.Native;
using Jint.Native.Array;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Client.Util;

namespace Raven.Server.Extensions
{
    public static class JintExtensions
    {
        public static IEnumerable<KeyValuePair<JsValue, PropertyDescriptor>> GetOwnPropertiesWithoutLength(this ArrayInstance array)
        {
            foreach (var kvp in array.GetOwnProperties())
            {
                if (kvp.Key == "length")
                    continue;

                yield return kvp;
            }
        }

        public static IDisposable ChangeMaxStatements(this Engine engine, int value)
        {
            var maxStatements = engine.FindConstraint<MaxStatements>();
            if (maxStatements == null)
                return null;

            var oldMaxStatements = maxStatements.Max;
            maxStatements.Change(value);

            return new DisposableAction(() => maxStatements.Change(oldMaxStatements));
        }

        public static IDisposable DisableMaxStatements(this Engine engine)
        {
            return ChangeMaxStatements(engine, int.MaxValue);
        }

        // this is a temporary solution to replace the implementation details of map and reduce containing modern JS features with stub functions and to switch off whole additional sources
        //
        // 1. like this before real implementations of mapDoc and reduceGroup functions for doc and grouping
        //  - mapDoc is optional just for return statements structure comparing
        //  - reduceGroup may just return null, no details are needed:
        /*JINT_START*/
        //const mapDoc = d => {if (1) {return {...}} elseif (2) { return {...}}; return {...} }
        //const reduceGroup = g => null
        /*JINT_END*/
        // here comes real implementations of mapDoc and reduceGroup functions
        // ...
        //
        // 2. like this to switch off an irrelevant additional source or its part: the whole file if in the first line or its part if somewhere in the body
        /*JINT_END*/
        // after this marker everything gets dropped for Jint
        //
        private static string ProcessJintStub(string script)
        {
            string res = "";

            string stubStart = "/*JINT_START*/";
            string stubEnd = "/*JINT_END*/";

            bool isStubStarted = false;
            bool isStubEnded = false;

            using (StringReader reader = new StringReader(script))
            {
                string line;
                while (!isStubEnded && (line = reader.ReadLine()) != null)
                {
                    if (!isStubEnded && line.Contains(stubEnd)) {
                        isStubEnded = true;
                    }
                    else if (!isStubStarted && line.Contains(stubStart)) {
                        isStubStarted = true;
                    }

                    int commentPos = (isStubStarted || isStubEnded) ? line.IndexOf("//") : -1;
                    res += "\r\n" + (commentPos >= 0 ? line.Substring(commentPos + 2, line.Length - (commentPos + 2)) : line);
                }
            }
            return res;
        }

        public static void ExecuteWithReset(this Engine engine, string source)
        {
            try
            {
                engine.Execute(JintExtensions.ProcessJintStub(source));
            }
            catch (JintException e) // all Jint errors can be ignored as we still may have access to AST
            {
            }
            finally
            {
                engine.ResetCallStack();
                engine.ResetConstraints();
            }
        }

        public static void ExecuteWithReset(this Engine engine, Script script)
        {
            try
            {
                engine.Execute(script);
            }
            catch (JintException e) // all Jint errors can be ignored as we still may have access to AST
            {
            }
            finally
            {
                engine.ResetCallStack();
                engine.ResetConstraints();
            }
        }

        public static string TryGetFieldFromSimpleLambdaExpression(this IFunction function)
        {
            if (!(function.Params.FirstOrDefault() is Identifier identifier))
                return null;

            var me = GetMemberExpression(function);
            if (me == null)
                return null;

            if (!(me.Property is Identifier property))
                return null;
            if ((!(me.Object is Identifier reference) || reference.Name != identifier.Name))
                return null;
            return property.Name;
        }

        private static MemberExpression GetMemberExpression(IFunction function)
        {
            switch (function)
            {
                case ArrowFunctionExpression afe:
                    return afe.ChildNodes.LastOrDefault() as StaticMemberExpression;
                default:
                    if (!(function.Body.ChildNodes.FirstOrDefault() is ReturnStatement rs))
                        return null;
                    return rs.Argument as MemberExpression;
            }
        }
    }
}
