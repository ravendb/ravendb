using System;
using System.Collections.Generic;
using System.IO;
using Spatial4n.Core.Context;
using Spatial4n.Core.Shapes;
using Spatial4n.Core.Util;

namespace Corax.Utils;

public class SpatialHelper
{
    //Source: https://github.com/apache/lucenenet/blob/master/src/Lucene.Net.Spatial/Query/SpatialArgs.cs
    private static readonly char[] alphabet = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm',
        'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
        'y', 'z'
    };

    private static readonly Dictionary<string, IRectangle> CachedFigures;
    private const int CachePrefixLength = 4;
    static SpatialHelper()
    {
        CachedFigures = new();
        Rec();

        void Rec(int lvl = 1, string parent = "")
        {
            if (lvl > CachePrefixLength)
                return;

            foreach (var a in alphabet)
            {
                var current = parent + a;
                CachedFigures.Add(current, GeohashUtils.DecodeBoundary(current, SpatialContext.GEO));
                Rec(lvl + 1, current);
            }
        }
    }
    
    internal static double GetErrorFromPercentage(SpatialContext ctx, IShape shape, double percentage)
    {
        if (percentage is < 0D or > 0.5D)
            throw new InvalidDataException("Error must be in [0; 0.5] range.");


        var boundingBox = shape.BoundingBox;
        //Our shape is inside the bounding box so lets calculate diagonal length of it;
        var diagonal = ctx.CalcDistance(ctx.MakePoint(boundingBox.MinX, boundingBox.MinY), boundingBox.MaxX, boundingBox.MaxY);
        return Math.Abs(diagonal) * percentage;
    }
    
    
    
    public static void FulfillShape(SpatialContext ctx, IShape shape, List<string> termMatch, List<string> listNeedsToBeCheckedExactly, int currentPrecision = 0, string currentGeohash = "", int maxPrecision = 9)
    {
        //There are several ways to fill in the shape. I chose the recursive way because it allows us not to worry about corner cases.
        //In this case, we do not have to worry that the centroid of the figure is outside the figure itself
        //(we are talking about creating the figure with BFS here).
        //Additionally, geohash uses Z-Order curves to create its indices, so it is not possible to iterate quite easily 4
        //(filling it with scan-line). Additionally, we get to the right parts of the world very quickly
        //(almost all of them fall off in the first iteration).

        
        //Notice this is only prototype. We have to get rid off all of this strings allocation
        if (currentPrecision > maxPrecision)
        {
            //This geohash to be check manually during querying.
            listNeedsToBeCheckedExactly.Add(currentGeohash);
            return;
        }

        foreach (var character in alphabet)
        {
            var geohashToCheck = currentGeohash + character;
            if (CachedFigures.TryGetValue(geohashToCheck, out var boundary) == false)
            {
                boundary = GeohashUtils.DecodeBoundary(geohashToCheck, Spatial4n.Core.Context.SpatialContext.GEO);
            }
            
            switch (shape.Relate(boundary))
            {
                case Spatial4n.Core.Shapes.SpatialRelation.CONTAINS:
                    //This is termmatch.
                    termMatch.Add(geohashToCheck);
                    break;
                case Spatial4n.Core.Shapes.SpatialRelation.WITHIN:
                case Spatial4n.Core.Shapes.SpatialRelation.INTERSECTS:
                    FulfillShape(ctx, shape, termMatch, listNeedsToBeCheckedExactly, currentPrecision + 1, geohashToCheck, maxPrecision);
                    continue;
                default:
                    continue;
            }
        }
    }
}
