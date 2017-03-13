using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyModel;
using Raven.Client.Documents.Exceptions;
using Sparrow.Json;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_6451
    {
        [Fact]
        public void Exceptions_should_not_have_blittable_and_pointer_fields()
        {
            //sanity check
            Assert.True(HasInvalidProperties(typeof(ClassWithPointer)));
            Assert.True(HasInvalidProperties(typeof(ClassWithNestedPointer)));
            Assert.True(HasInvalidProperties(typeof(ClassWithBlittable)));
            Assert.True(HasInvalidProperties(typeof(ClassWithNestedClassWithBlittable)));

            var referenceAssemblies = GetRuntimeNonMicrosoftAssemblies();
            var exceptionTypes =
                (from type in referenceAssemblies.SelectMany(x => x.ExportedTypes)
                    where typeof(Exception).IsAssignableFrom(type) &&
                          (type.Namespace.StartsWith("Raven") ||
                           type.Namespace.StartsWith("Voron") ||
                           type.Namespace.StartsWith("Sparrow"))
                    select type).ToArray();

            foreach (var t in exceptionTypes)
            {
                try
                {
                    if(t == typeof(DocumentConflictException))
                        Debugger.Break();
                    Assert.False(HasInvalidProperties(t), $"The type {t.FullName} should not have pointer or BlittableXXX properties");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }

        public bool HasInvalidProperties(Type t)
        {
            var properties = t.GetProperties();
            foreach (var p in properties)
            {
                if (p.PropertyType.IsPointer)
                    return true;

                if (p.PropertyType.Name.Contains("Blittable"))
                    return true;

                if (p.PropertyType.IsArray ||
                    p.PropertyType.Namespace.StartsWith("System") ||
                    p.PropertyType.GetTypeInfo().IsEnum ||
                    p.PropertyType.GetTypeInfo().IsPrimitive)
                {
                    continue;
                }

                if (p.PropertyType.GetTypeInfo().IsPrimitive == false &&
                    IsIEnumerable(p.PropertyType) == false &&
                    p.PropertyType.GetTypeInfo().IsInterface == false &&
                    p.PropertyType.GetTypeInfo().ContainsGenericParameters == false &&
                    typeof(IEnumerable).IsAssignableFrom(p.PropertyType) == false &&
                    HasInvalidProperties(p.PropertyType))
                {
                    return true;
                }
            }        

            return false;
        }

        public bool IsIEnumerable(Type t)
        {
            if (t.IsConstructedGenericType)
            {
                var genericType = t.GetGenericTypeDefinition();
                if(genericType.GetInterfaces().Any(x => x.IsConstructedGenericType && 
                                                        x.GetGenericTypeDefinition() == typeof(IEnumerable<>)))                    
                    return true;
            }
            return false;
        }

        //slightly modified version from to http://www.michael-whelan.net/replacing-appdomain-in-dotnet-core/
        public static IEnumerable<Assembly> GetRuntimeNonMicrosoftAssemblies()
        {
            var assemblies = new List<Assembly>();
            var dependencies = DependencyContext.Default.RuntimeLibraries;
            foreach (var library in dependencies)
            {
                if (library.Name.StartsWith("Microsoft") ||
                    library.Name.StartsWith("NuGet") ||
                    library.Name.StartsWith("System"))
                    continue;
                try
                {
                    var assembly = Assembly.Load(new AssemblyName(library.Name));
                    assemblies.Add(assembly);
                }
                catch (FileNotFoundException)
                {
                    //if assembly is not found --> it is not directly referenced
                }
            }
            return assemblies;
        }


        public unsafe class ClassWithPointer
        {
            public List<string> JustAList { get; set; }

            public byte* Ptr { get; set; }
        }

        public class ClassWithNestedPointer
        {
            public ClassWithPointer CPtr { get; set; }
        }

        public class ClassWithBlittable
        {
            public BlittableJsonReaderObject Json { get; set; }
        }

        public class ClassWithNestedClassWithBlittable
        {
            public ClassWithBlittable CJson { get; set; }
        }
    }
}
