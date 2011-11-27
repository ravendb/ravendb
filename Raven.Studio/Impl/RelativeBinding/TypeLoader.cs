

// this class is documented here:
// http://blog.thekieners.com/2010/09/06/type-gettype-implementation-with-help-of-xamlreader/


using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Windows;
using System.Windows.Markup;
using System.Windows.Media;

namespace Nova.Core.Common
{

    /// <summary>
    /// Provides functionality to load any type with its class name, namespace and assembly-name within the Silverlight environment.
    /// </summary>
    /// <remarks>
    /// The Type.GetType method is different in Silverlight than in the standard .NET runtime. In Silverlight we have to provide the 
    /// fully qualified assembly name to get a type in a custom assembly. Only build in controls or types in the same assembly are 
    /// excluded from this rule. Full qualified assembly name means a syntax like the following: 
    /// MyComponent.MyType, MyAssembly, Version=1.0.0.0, Culture=neutral, PublicKeyToken=4bec85d7bec6698f.
    /// This class uses the XamlReader capability to resolve type during parsing a xaml-string. While this is a little time consuming
    /// the TypeLoader maintains a cache to get types faster.
    /// </remarks>
    public static class TypeLoader
    {
        // cache for resolved type
        private static Dictionary<string, Type> _cache = new Dictionary<string, Type>();

        /// <summary>
        /// Gets the System.Type with the specified name, name space and assembly name.
        /// </summary>
        /// <param name="className">The class name without namespace.</param>
        /// <param name="nameSpace">The name space</param>
        /// <param name="assemblyName">The name of the assembly containing the type.</param>
        /// <returns>The type matching the provided parameters or null if not found.</returns>
        //[DebuggerStepThrough()]
        public static Type GetType(string className, string nameSpace, string assemblyName)
        {
            // check
            if (StringHelper.IsNullOrWhiteSpace(nameSpace))
                return null;

            string xamlNamespace = string.Format("clr-namespace:{0}", nameSpace);
            // assembly name is optional
            if (!StringHelper.IsNullOrWhiteSpace(assemblyName))
                xamlNamespace += string.Format(";assembly={0}", assemblyName);

            return GetType(className, xamlNamespace);
        }

        /// <summary>
        /// Gets the System.Type with the specified name. 
        /// This method overload can be used for:
        /// 1. core controls such as Button, Grid, ListBox, etc. without specifying the namespace or assembly name.
        /// 2. with the qualified assembly name of the type without version and public key token like this: "MyNamespace.MyType, MyAssembly".
        /// </summary>
        /// <param name="className">Pure class name of Core Controls such as Button, Grid, ListBox, etc.</param>
        /// <returns>The type matching the provided parameters or null if not found.</returns>
        //[DebuggerStepThrough()]
        public static Type GetType(string className)
        {
            if (className != null && className.Contains(","))
            {
                string[] qualifiedNameParts = className.Split(',');

                if (qualifiedNameParts.Length == 2)
                {
                    string[] fullClassNameParts = qualifiedNameParts[0].Split('.');

                    if (fullClassNameParts.Length > 0)
                    {
                        // classname
                        string newClassName = fullClassNameParts.Last().Trim();

                        // namespace
                        string nameSpace = "";
                        for (int i = 0; i < fullClassNameParts.Length - 1; i++)
                        {
                            nameSpace += fullClassNameParts[i] + ".";
                        }
                        nameSpace = nameSpace.TrimEnd('.');

                        string assemblyName = qualifiedNameParts[1].Trim();

                        return GetType(newClassName, nameSpace, assemblyName);
                    }
                }

            }

            return GetType(className, "");
        }

        /// <summary>
        /// Gets the System.Type with the specified name. The xaml namespace specifies the namespace and assembly name in the same syntax as in xaml. 
        /// </summary>
        /// <param name="className">The class name without namespace.</param>
        /// <param name="xamlNamespace">
        /// The xaml namespace. This is the same syntax as used in XAML syntax. 
        /// Example: "clr-namespace:MyComponent.SubNamespace;assembly=MyAssemblyName
        /// </param>
        /// <returns>The type matching the provided parameters or null if not found.</returns>
        //[DebuggerStepThrough()]
        public static Type GetType(string className, string xamlNamespace)
        {
            // check input
            if (StringHelper.IsNullOrWhiteSpace(className))
                return null;

            if (className.Contains("."))
                throw new ArgumentException("className must not include the namespace. Please provide namespace with separate parameter.");

            // check if type is already in cache
            string key = xamlNamespace + "&" + className;

            if (_cache.ContainsKey(key))
                return _cache[key];


            lock (_cache)
            {
                try
                {
                    // check again because another thread might be faster and has already created the cache-entry
                    if (_cache.ContainsKey(key))
                        return _cache[key];

                    // create xaml with a simply Style element and set the TargetType property with the provided type name
                    string xaml = "<Style xmlns='http://schemas.microsoft.com/winfx/2006/xaml/presentation' ";

                    // set the xaml namesapce if provided
                    if (!StringHelper.IsNullOrWhiteSpace(xamlNamespace))
                    {
                        xaml += string.Format("xmlns:tmp='{0}' TargetType='tmp:{1}' />", xamlNamespace, className);
                    }
                    else
                    {
                        // Core controls such as Button, Grid, ListBox, etc do not need a namespace
                        xaml += string.Format("TargetType='{0}' />", className);
                    }

                    // let the XamlParser load the type via the TargetType property 
                    Style style = XamlReader.Load(xaml) as Style;

                    if (style != null)
                    {
                        Type targetType = style.TargetType;
                        AddToCache(key, targetType);
                        return targetType;
                    }
                }
                catch (Exception ex)
                {
                    // Try to load type in executing assembly
                    if (!StringHelper.IsNullOrWhiteSpace(xamlNamespace))
                    {
                        // note: Type.GetType uses needs assembly-qualified name of the type to get. If the type is 
                        //       in the currently executing assembly or in Mscorlib.dll, it is sufficient to supply 
                        //       the type name qualified by its namespace.
                        Type type = Type.GetType(string.Format("{0}.{1}", xamlNamespace.Replace("clr-namespace:", "").TrimEnd(';'), className));

                        if (type != null)
                        {
                            // add to cache
                            AddToCache(key, type);
                            return type;
                        }
                    }

                    //****** DONT SET VALUE TO NULL, BECAUSE OF CASES WHEN AN ASSEMBLY IS  *****
                    //****** LOADED DYNAMICALLY INTO THE APPLICATION DOMAIN                *****
                    // don't let the exception repeat. Set null as cache value
                    AddToCache(key, null);
                    //**************************************************************************/
                }
            }

            return null;
        }

        private static void AddToCache(string key, Type type)
        {
            _cache.Add(key, type);
            CompositionTarget.Rendering -= new EventHandler(CompositionTarget_Rendering);
            CompositionTarget.Rendering += new EventHandler(CompositionTarget_Rendering);
        }

        static void CompositionTarget_Rendering(object sender, EventArgs e)
        {
            CompositionTarget.Rendering -= new EventHandler(CompositionTarget_Rendering);
            _cache.Clear();
        }

        private static class StringHelper
        {
            // helper because .NET 3.5 does not support IsNullOrWhiteSpace
            public static bool IsNullOrWhiteSpace(string str)
            {
                if (str == null)
                    return true;

                if (str.Trim() == string.Empty)
                    return true;

                return false;
            }
        }

    }
}
