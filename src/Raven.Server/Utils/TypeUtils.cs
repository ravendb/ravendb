using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using CsvHelper;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    
    public static class TypeUtils
    {
        private struct VisitedResetBehavior : IResetSupport<HashSet<object>>
        {
            public void Reset(HashSet<object> value)
            {
                value.Clear();
            }
        }

        private static readonly ObjectPool<HashSet<object>,VisitedResetBehavior> VisitedHashsets = 
            new ObjectPool<HashSet<object>, VisitedResetBehavior>(() => new HashSet<object>(Sparrow.Utils.ReferenceEqualityComparer.Default));

        //detect if an object is a blittable or has any blittable objects somewhere in its object hierarchy
        public static bool ContainsBlittableObject(this object obj)
        {
            var visited = VisitedHashsets.Allocate();
            try
            {
                return obj.ContainsBlittableObject(visited);
            }
            finally
            {
                VisitedHashsets.Free(visited);
            }
        }

        //credit : modified code from https://stackoverflow.com/q/17520839
        private static bool ContainsBlittableObject(this object obj, HashSet<object> visited)
        {
            if (obj == null)
                return false;

            //prevent infinite loops
            if (visited.Contains(obj))
                return false;

            visited.Add(obj);

            var type = obj.GetType();

            if (type.IsPointer || type.IsEnum || type.IsCOMObject || type.IsValueType || type == typeof(string)) //obviously not what we are looking for
                return false;

            if (obj is BlittableJsonReaderObject || obj is BlittableJsonReaderArray)
                return true;
            
            if (obj is IEnumerable array)
            {
                foreach (var item in array)
                {
                    if (item.ContainsBlittableObject(visited))
                    {
                        return true;
                    }
                }

                return false;
            }

            if (obj is ITuple tuple)
            {
                for (int i = 0; i < tuple.Length; i++)
                {
                    if (tuple[i].ContainsBlittableObject(visited))
                    {
                        return true;
                    }
                }

                return false;
            }

            if (type.IsClass || type.IsUserDefinedStruct())
            {
                var fields = type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                foreach (var field in fields)
                {
                    var item = field.GetValue(obj);
                    if (item?.ContainsBlittableObject(visited) ?? false)
                    {
                        return true;
                    }
                }

                var properties = type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                foreach (var value in properties.Where(p => p.CanRead).Select(p => p.GetValue(obj)))
                {
                    if (value?.ContainsBlittableObject(visited) ?? false)
                    {
                        return true;
                    }
                }

                return false;
            }

            return false;
        }
    }
}
