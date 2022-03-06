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
        public const string ExecEnvCodeJint = @"
var process = {
    env: {
        EXEC_ENV: 'RavenDB',
        ENGINE: 'Jint'
    }
}
";
        public JintEngineExForV8()
        {
            ExecuteWithReset(ExecEnvCodeJint, "ExecEnvCode");
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
                Execute(source);
            }
            /*catch (JintException) // all Jint errors can be ignored as we still may have access to AST (if we don't then we will detect it later)
            {
            }*/
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
            /*catch (JintException) // all Jint errors can be ignored as we still may have access to AST (if we don't then we will detect it later)
            {
            }*/
            finally
            {
                ResetCallStack();
                ResetConstraints();
            }
        }
    }
}
