using System;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document.Blittable
{
    public static class BlittableExtentions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="self"></param>
        /// <param name="path"></param>
        /// <param name="createSnapshots">Set to true if you want to modify selected objects</param>
        /// <returns></returns>
        public static IEnumerable<Tuple<object, object>> SelectTokenWithRavenSyntaxReturningFlatStructure(this BlittableJsonReaderBase self, string path, bool createSnapshots = false)
        {
            var pathParts = path.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            object result = null;
            result = new BlitPath(pathParts[0]).Evaluate(self, false);

            if (pathParts.Length == 1)
            {
                yield return Tuple.Create(result, (object)self);
                yield break;
            }

            if (result is BlittableJsonReaderObject)
            {
                var blitResult = result as BlittableJsonReaderObject;
                blitResult.TryGetMember(Constants.Json.Fields.Values, out result);

                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (var i = 0; i < blitResult.Count; i++)
                {
                    blitResult.GetPropertyByIndex(i, ref prop);

                    if (prop.Value is BlittableJsonReaderBase)
                    {
                        var itemAsBlittable = prop.Value as BlittableJsonReaderBase;
                        foreach (var subItem in itemAsBlittable.SelectTokenWithRavenSyntaxReturningFlatStructure(string.Join(",", pathParts.Skip(1).ToArray())))
                        {
                            yield return subItem;
                        }
                    }
                    else
                    {
                        yield return Tuple.Create((object)prop.Value , result);
                    }
                }
            }
            else if (result is BlittableJsonReaderArray)
            {
                var blitResult = result as BlittableJsonReaderArray;
                for (var i = 0; i < blitResult.Length; i++)
                {
                    var item = blitResult[i];

                    if (item is BlittableJsonReaderBase)
                    {
                        var itemAsBlittable = item as BlittableJsonReaderBase;
                        foreach (var subItem in itemAsBlittable.SelectTokenWithRavenSyntaxReturningFlatStructure(string.Join(",", pathParts.Skip(1).ToArray())))
                        {
                            yield return subItem;
                        }
                    }
                    else
                    {
                        yield return Tuple.Create((object)item, result);
                    }
                }
            }
            else
            {
                throw new ArgumentException("Illegal path");
            }
        }
    }
}
