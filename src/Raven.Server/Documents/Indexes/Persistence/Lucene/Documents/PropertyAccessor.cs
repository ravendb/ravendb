using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public delegate object DynamicGetter(object target);

    public class PropertyAccessor
    {
        public readonly Dictionary<string, DynamicGetter> Properties = new Dictionary<string, DynamicGetter>();

        public static PropertyAccessor Create(Type type)
        {
            var accessor = new PropertyAccessor();

            foreach (var prop in type.GetProperties())
            {
                accessor.Properties.Add(prop.Name, CreateGetMethod(prop, type));
            }

            return accessor;
        }

        public object GetValue(string name, object target)
        {
            DynamicGetter getterMethod;
            if (Properties.TryGetValue(name, out getterMethod))
                return getterMethod(target);

            throw new InvalidOperationException(string.Format("The {0} property was not found", name));
        }

        private static DynamicGetter CreateGetMethod(PropertyInfo propertyInfo, Type type)
        {
            var getMethod = propertyInfo.GetGetMethod();

            if (getMethod == null)
                throw new InvalidOperationException(string.Format("Could not retrieve GetMethod for the {0} property of {1} type", propertyInfo.Name, type.FullName));

            var arguments = new Type[1]
            {
                typeof (object)
            };

            var getterMethod = new DynamicMethod(string.Concat("_Get", propertyInfo.Name, "_"), typeof(object), arguments, propertyInfo.DeclaringType);
            var generator = getterMethod.GetILGenerator();

            generator.DeclareLocal(typeof(object));
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Castclass, propertyInfo.DeclaringType);
            generator.EmitCall(OpCodes.Callvirt, getMethod, null);

            if (propertyInfo.PropertyType.GetTypeInfo().IsClass == false)
                generator.Emit(OpCodes.Box, propertyInfo.PropertyType);

            generator.Emit(OpCodes.Ret);

            return (DynamicGetter)getterMethod.CreateDelegate(typeof(DynamicGetter));
        }
    }
}
