using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Sparrow.Extensions
{
    public static class TypeExtensions
    {
        internal const string RecordEqualityContractPropertyName = "EqualityContract";

        public static string GetTypeNameForSerialization(this Type t)
        {
            return RemoveAssemblyDetails(t.AssemblyQualifiedName);
        }

        internal static bool IsRecord(this Type type)
        {
            if (type == null)
                return false;

            var equalityContractProperty = type.GetProperty(RecordEqualityContractPropertyName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            if (equalityContractProperty == null)
                return false;

            var getMethod = equalityContractProperty.GetGetMethod(nonPublic: true);
            if (getMethod == null)
                return false;

            return getMethod.GetCustomAttribute(typeof(CompilerGeneratedAttribute)) != null;
        }

        private static string RemoveAssemblyDetails(string fullyQualifiedTypeName)
        {
            StringBuilder builder = new StringBuilder();

            // loop through the type name and filter out qualified assembly details from nested type names
            bool writingAssemblyName = false;
            bool skippingAssemblyDetails = false;
            for (int i = 0; i < fullyQualifiedTypeName.Length; i++)
            {
                char current = fullyQualifiedTypeName[i];
                switch (current)
                {
                    case '[':
                        writingAssemblyName = false;
                        skippingAssemblyDetails = false;
                        builder.Append(current);
                        break;

                    case ']':
                        writingAssemblyName = false;
                        skippingAssemblyDetails = false;
                        builder.Append(current);
                        break;

                    case ',':
                        if (!writingAssemblyName)
                        {
                            writingAssemblyName = true;
                            builder.Append(current);
                        }
                        else
                        {
                            skippingAssemblyDetails = true;
                        }
                        break;

                    default:
                        if (!skippingAssemblyDetails)
                            builder.Append(current);
                        break;
                }
            }

            return builder.ToString();
        }
    }
}
