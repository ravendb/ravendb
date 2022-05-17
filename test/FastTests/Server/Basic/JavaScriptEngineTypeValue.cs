using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide.JavaScript;
using Xunit;

namespace FastTests.Server.JavaScript
{
    public enum JavaScriptEngineClassDataMode
    {
        OnlyJint,
        OnlyV8,
        AllEngines
    }
    
    public class JavaScriptEngineClassDataAttribute : ClassDataAttribute
    {
        public JavaScriptEngineClassDataAttribute() : base(typeof(JavaScriptEngineMode.AllJavaScriptEngines))
        {
        }

        private static Type GetType(JavaScriptEngineType type)
            => type switch
            {
                JavaScriptEngineType.Jint => typeof(JavaScriptEngineMode.JavaScriptEngineJint),
                JavaScriptEngineType.V8 => typeof(JavaScriptEngineMode.JavaScriptEngineV8),
                _ => typeof(JavaScriptEngineMode.AllJavaScriptEngines),
            };
        public JavaScriptEngineClassDataAttribute(JavaScriptEngineType useOnlyEngineType) : base(GetType(useOnlyEngineType))
        {
        }
    }

    public class JavaScriptEngineMode
    {
        public static JavaScriptEngineType Parse(string value)
        {
            return ((IEnumerable<JavaScriptEngineType>)Enum.Parse(typeof(JavaScriptEngineType), value)).First();
        }
    
        public class AllJavaScriptEngines : JavaScriptEngineTypeData
        {
            public AllJavaScriptEngines()
            {
                _data = new()
                {
                    new object[] {JavaScriptEngineType.Jint},
                    new object[] {JavaScriptEngineType.V8}
                };
            }
        }
        
        public class JavaScriptEngineJint : JavaScriptEngineTypeData
        {
            public JavaScriptEngineJint()
            {
                _data = new()
                {
                    new object[] {JavaScriptEngineType.Jint}
                };
            }
        }
        
        public class JavaScriptEngineV8 : JavaScriptEngineTypeData
        {
            public JavaScriptEngineV8()
            {
                _data = new()
                {
                    new object[] {JavaScriptEngineType.V8}
                };
            }
        }
        
        public abstract class JavaScriptEngineTypeData : IEnumerable<object[]>
        {
            protected List<object[]> _data;
                

            public IEnumerator<object[]> GetEnumerator() => _data.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }
}
