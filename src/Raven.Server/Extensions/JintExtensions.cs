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

        public static string JintStubInstruction = 
@"To use some of the modern JS features like optional chaining you should add stubs for map and/or reduce functions and/or switch off whole additional sources with implementation details that are not used in the stubs.
Also please make shure that you have available to Jint fully workable (without modern JS features) definitions of all the functions and other code that you use to return map and reduce expressions (if you use them).

1. You can switch off or replace pieces of code based on the environment variable process.env.ENGINE which is set to 'Jint' for Jint engine:
const IS_ENGINE_JINT = process && process.env && process.env.ENGINE === 'Jint'
if (!IS_ENGINE_JINT) {
    // switched off code
}
else { // optional
    // replacement code for Jint
}

2. Like this before real implementations of mapDoc and reduceGroup functions for doc and grouping:
map(colName, mapDoc)
groupBy(x => ({ ... })).aggregate(reduceGroup)
/*JINT_END*/ // after this marker everything gets dropped for Jint
// here real implementations of mapDoc and reduceGroup functions start
...

Or like this if you have to close some outer block, for example:
/*JINT_START*/ // after this marker starts stub code for Jint which will be uncommented
//}
/*JINT_END*/ // starting from this marker everything gets dropped for Jint
// here real implementations of mapDoc and reduceGroup functions start
...

Don't use '//' comment lines in the stub block as the first occurences of '//' will be removed to get the stub code.

MapDoc is optional as it is used for checking of map return statements' structure consistency only which can be omitted if you don't need it.
No implementation details of ReduceGroup are used and only groupBy's argument matters.

Actually, we could define stub definitions for Jint them like below, but appears to be redundant as it will work even without it (as above):
/*JINT_START*/
//const mapGroup = g => null
//const reduceGroup = g => null
/*JINT_END*/

In case you want checking of map return statements' structure consistency to be performed you should add mapDoc stub with all your return structures described as well like this:
/*JINT_START*/
//const mapDoc = d => {
//    if (1) {
//        return {...}
//    } elseif (2) { 
//        return {...}
//    } 
//    return {...} 
//}
/*JINT_END*/

3. Like this to switch off an irrelevant additional source or its part: the whole file if in the first line or its lower part if somewhere in the body.
Actually, it is worth to switch off the whole file in case it does not contain map/reduce definitions for the index in place.
/*JINT_END*/ // after this marker everything gets dropped for Jint
";


        // this is a temporary solution based on the description in the above JintStubInstruction string (till we don't have the latest Esprima version integrated)
        public static string ProcessJintStub(string script)
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
                engine.Execute(ProcessJintStub(source));
            }
            catch (JintException e) // all Jint errors can be ignored as we still may have access to AST (if we don't then we will detect it later)
            {
            }
            catch (Exception e)
            {
                throw new Exception(JintStubInstruction, e);
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
            catch (JintException e) // all Jint errors can be ignored as we still may have access to AST (if we don't then we will detect it later)
            {
            }
            catch (Exception e)
            {
                throw new ArgumentException(JintStubInstruction, e);
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
