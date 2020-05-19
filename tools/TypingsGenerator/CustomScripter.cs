using System;
using System.Reflection;
using TypeScripter;
using TypeScripter.TypeScript;

namespace TypingsGenerator
{
    public class CustomScripter : Scripter
    {
        protected override TsName GetName(Type type)
        {
            var tsName = base.GetName(type);
            var typeInfo = type;

            if (typeInfo.IsNested)
            {
                // if type is nested, use fullName of declaring type
                // it will have form: Raven.Server.OuterClass+DeclaringClass
                // replace '+' with '.'

                return new TsName(tsName.Name, type.DeclaringType.FullName.Replace("+", "."));
            }

            return tsName;
        }
    }
}