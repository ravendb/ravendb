namespace Raven.Management.Client.Silverlight.Document
{
    using System;
    using System.Text;

    /// <summary>
    /// Helper class for reflection operations
    /// </summary>
    public static class ReflectionUtil
    {
        /// <summary>
        /// Gets the full name without version information.
        /// </summary>
        /// <param name="entityType">Type of the entity.</param>
        /// <returns></returns>
        public static string GetFullNameWithoutVersionInformation(Type entityType)
        {
            string asmName = entityType.Name;
            //var asmName = entityType.Assembly.GetName().Name;
            if (entityType.IsGenericType)
            {
                Type genericTypeDefinition = entityType.GetGenericTypeDefinition();
                var sb = new StringBuilder(genericTypeDefinition.FullName);
                sb.Append("[");
                foreach (Type genericArgument in entityType.GetGenericArguments())
                {
                    sb.Append("[")
                        .Append(GetFullNameWithoutVersionInformation(genericArgument))
                        .Append("]");
                }
                sb.Append("], ")
                    .Append(asmName);
                return sb.ToString();
            }
            return entityType.FullName + ", " + asmName;
        }
    }
}