using System;
using System.Runtime.CompilerServices;
using System.IO;
using System.Threading;
using Esprima.Ast;
using Jint;
using Jint.Constraints;
using Jint.Runtime;
using Raven.Client.Util;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Extensions.V8
{
    public class JintEngineExForV8 : Engine, IJavaScriptEngineForParsing
    {
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
                    if (!isStubEnded && line.Contains(stubEnd))
                        isStubEnded = true;
                    else if (!isStubStarted && line.Contains(stubStart))
                        isStubStarted = true;

                    int commentPos = (isStubStarted || isStubEnded) ? line.IndexOf("//") : -1;
                    res += "\r\n" + (commentPos >= 0 ? line.Substring(commentPos + 2, line.Length - (commentPos + 2)) : line);
                }
            }
            return res;
        }
        
        public IDisposable DisableConstraints()
        {
            var disposeMaxStatements = ChangeMaxStatements(0);
            var disposeMaxDuration = ChangeMaxDuration(0);

            void Restore()
            {
                disposeMaxStatements?.Dispose();
                disposeMaxDuration?.Dispose();
            }
            
            return new DisposableAction(Restore);
        }

        public IDisposable ChangeMaxStatements(int value)
        {
            var maxStatements = FindConstraint<MaxStatements>();
            if (maxStatements == null)
                return null;

            var oldMaxStatements = maxStatements.Max;
            maxStatements.Change(value == 0 ? int.MaxValue : value);

            return new DisposableAction(() => maxStatements.Change(oldMaxStatements));
        }

        public IDisposable ChangeMaxDuration(int value)
        {
            return ChangeMaxDuration(value == 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(value));
        }

        public IDisposable ChangeMaxDuration(TimeSpan value)
        {
            //return new DisposableAction(() => { }); 
                
            var maxDuration = FindConstraint<TimeConstraint2>(); // TODO [shlomo] to expose in Jint TimeConstraint2 that is now internal, add Change method to it and replace MaxStatements to TimeConstraint2
            if (maxDuration == null)
                return null;

            var oldMaxDuration = maxDuration.Timeout;
            maxDuration.Change(value); // TODO [shlomo] to replace on switching to TimeConstraint2: TimeSpan.FromMilliseconds(value == 0 ? int.MaxValue : value));

            return new DisposableAction(() => maxDuration.Change(oldMaxDuration));
        }
        
        public JsHandle GlobalObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Realm.GlobalObject);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetGlobalProperty(string propertyName)
        {
            return new JsHandle(GetValue(propertyName));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetGlobalProperty(string name, JsHandle value)
        {
            SetValue(name, value.Jint);
        }
        
        public void Execute(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true)
        {
            if (throwExceptionOnError)
                base.Execute(source);
            else
            {
                try
                {
                    base.Execute(source);
                }
                catch
                {
                }
            }
        }

        public void ExecuteWithReset(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true)
        {
            try
            {
                Execute(ProcessJintStub(source));
            }
            catch (JintException) // all Jint errors can be ignored as we still may have access to AST (if we don't then we will detect it later)
            {
            }
            catch (Exception e)
            {
                throw new Exception(JintStubInstruction, e);
            }
            finally
            {
                ResetCallStack();
                ResetConstraints();
            }
        }

        public void ExecuteWithReset(Script script)
        {
            try
            {
                Execute(script);
            }
            catch (JintException) // all Jint errors can be ignored as we still may have access to AST (if we don't then we will detect it later)
            {
            }
            catch (Exception e)
            {
                throw new ArgumentException(JintStubInstruction, e);
            }
            finally
            {
                ResetCallStack();
                ResetConstraints();
            }
        }
    }
}
