using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Server;
using Spatial4n.Context;
using Spatial4n.Distance;
using Spatial4n.Shapes;
using Spatial4n.Util;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Utils.Spatial;

public sealed class SpatialUtils
{
    /// <summary>
    ///Source: https://github.com/apache/lucenenet/blob/master/src/Lucene.Net.Spatial/Query/SpatialArgs.cs
    /// </summary>
    private static readonly char[] Alphabet =
    {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };

    /// <summary>
    ///Source: https://github.com/apache/lucenenet/blob/master/src/Lucene.Net.Spatial/Query/SpatialArgs.cs
    /// </summary>
    internal static double GetErrorFromPercentage(SpatialContext ctx, IShape shape, double percentage)
    {
        if (percentage is < 0D or > 0.5D)
            throw new InvalidDataException("Error must be in [0; 0.5] range.");


        var boundingBox = shape.BoundingBox;
        //Our shape is inside the bounding box so lets calculate diagonal length of it;
        var diagonal = ctx.CalcDistance(ctx.MakePoint(boundingBox.MinX, boundingBox.MinY), boundingBox.MaxX, boundingBox.MaxY);
        return Math.Abs(diagonal) * percentage;
    }


    public const int DefaultGeohashLevel = 9;
    private static readonly Dictionary<string, IRectangle> CachedFigures = new ();

    /// <summary>
    ///  Minimum amount of terms in specific area required to start compressing query into smaller ones.
    /// </summary>
    private const int Threshold = 2 << 10;

    public static double GetGeoDistance(double lat, double lng, double center_lng, double center_lat) =>
        GetGeoDistance((lat, lng), (center_lng, center_lat), 0, SpatialUnits.Kilometers);

    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double GetGeoDistance(in (double lat, double lng) fieldCoordinates, (double X, double Y) center, double round, SpatialUnits units)
    {
        var distance = HaverstineDistanceInInternationalNauticalMiles(center.Y, center.X, fieldCoordinates.lat, fieldCoordinates.lng);
        const double NumberOfMilesInNauticalMile = 1.1515;
        const double NumberOfKilometersInNauticalMile = 1.852;

        distance = units switch
        {
            SpatialUnits.Miles => distance * NumberOfMilesInNauticalMile,
            SpatialUnits.Kilometers => distance * NumberOfKilometersInNauticalMile,
            _ => distance
        };

        if (round <= 0)
            return distance;
        
        return distance - distance % round;
    }

    internal static double HaverstineDistanceInInternationalNauticalMiles(double lat1, double lng1, double lat2, double lng2)
    {
        // from : https://www.geodatasource.com/developers/javascript
        if ((lat1 == lat2) && (lng1 == lng2))
        {
            return 0;
        }
        else
        {
            var radlat1 = DistanceUtils.DegreesToRadians * lat1;
            var radlat2 = DistanceUtils.DegreesToRadians * lat2;
            var theta = lng1 - lng2;
            var radtheta = DistanceUtils.DegreesToRadians * theta;
            var dist = Math.Sin(radlat1) * Math.Sin(radlat2) + Math.Cos(radlat1) * Math.Cos(radlat2)
                                                                                 * Math.Cos(radtheta);
            if (dist > 1)
            {
                dist = 1;
            }

            dist = Math.Acos(dist);
            dist = dist * DistanceUtils.RadiansToDegrees;

            const double NauticalMilePerDegree = 60;
            return dist * NauticalMilePerDegree;
        }
    }


    public static IEnumerable<(string Geohash, bool IsTermMatch)> GetGeohashesForQueriesOutsideShape(Querying.IndexSearcher searcher, CompactTree tree,
        ByteStringContext allocator,
        SpatialContext ctx,
        IShape shape, int currentPrecision = 0, string currentGeohash = "", int maxPrecision = DefaultGeohashLevel)
    {
        if (currentPrecision > maxPrecision)
        {
            yield return (currentGeohash, false);
        }
        else
        {
            foreach (var character in Alphabet)
            {
                var geohashToCheck = currentGeohash + character;
                if (CachedFigures.TryGetValue(geohashToCheck, out var boundary) == false)
                {
                    boundary = GeohashUtils.DecodeBoundary(geohashToCheck, ctx);
                }

                switch (boundary.Relate(shape))
                {
                    case Spatial4n.Shapes.SpatialRelation.Disjoint:
                        //Our termatch
                        using (var _ = Slice.From(allocator, geohashToCheck, out var term))
                        {
                            var amount = searcher.NumberOfDocumentsUnderSpecificTerm(tree, term);
                            if (amount == 0)
                            {
                                continue;
                            }


                            yield return (geohashToCheck, true);
                        }

                        break;
                    
                    //Our figure contains whole boundary so we've to skip.
                    case Spatial4n.Shapes.SpatialRelation.Within:
                        break;
                    //Contains means our figure is within boundary, we've to go deeper to find out more information about it.
                    case Spatial4n.Shapes.SpatialRelation.Contains:
                    case Spatial4n.Shapes.SpatialRelation.Intersects:
                        using (var _ = Slice.From(allocator, geohashToCheck, out var term))
                        {
                            var amount = searcher.NumberOfDocumentsUnderSpecificTerm(tree, term);
                            if (amount == 0)
                            {
                                continue;
                            }

                            if (amount <= Threshold)
                            {
                                yield return (geohashToCheck, false);
                            }
                            else
                            {
                                var innerGeohashes = GetGeohashesForQueriesOutsideShape(searcher, tree, allocator, ctx, shape, currentPrecision + 1, geohashToCheck,
                                    maxPrecision);

                                foreach (var geohash in innerGeohashes)
                                {
                                    yield return geohash;
                                }
                            }
                        }

                        break;
                    default:
                        break;
                }
            }
        }
    }


    //There are several ways to fill in the shape. I chose the recursive way because it allows us not to worry about corner cases.
    //In this case, we do not have to worry that the centroid of the figure is outside the figure itself
    //(we are talking about creating the figure with BFS here).
    //Additionally, geohash uses Z-Order curves to create its indices, so it is not possible to iterate quite easily 4
    //(filling it with scan-line). Additionally, we get to the right parts of the world very quickly
    //(almost all of them fall off in the first iteration).
    // Additionally we will do compression only if it's needed (when term amount is above Threshold) so we can call it LazyCompression.
    // If amount is under our threshold we will check every item with Spatial4n.Relate

    //Example of compression is available here:
    //https://user-images.githubusercontent.com/86351904/166667938-5eda4fd4-d69b-4b71-8639-d4447abec0e7.png
    //https://user-images.githubusercontent.com/86351904/166668006-31d0f5e5-70c1-4a7c-820d-c933e5d2960d.png
    //https://user-images.githubusercontent.com/86351904/166668024-ab05247b-68c1-45ae-a17e-3e7d5f6f4c50.png
    //https://user-images.githubusercontent.com/86351904/166668035-e9f94e0a-59ed-42fa-bcfe-c33d7a87d02d.png
    public static IEnumerable<(string Geohash, bool IsTermMatch)> GetGeohashesForQueriesInsideShape(Querying.IndexSearcher searcher, CompactTree tree, ByteStringContext allocator,
        SpatialContext ctx,
        IShape shape, int currentPrecision = 0, string currentGeohash = "", int maxPrecision = DefaultGeohashLevel)
    {
        if (currentPrecision > maxPrecision)
        {
            yield return (currentGeohash, false);
        }
        else
        {
            foreach (var character in Alphabet)
            {
                var geohashToCheck = currentGeohash + character;
                if (CachedFigures.TryGetValue(geohashToCheck, out var boundary) == false)
                {
                    boundary = GeohashUtils.DecodeBoundary(geohashToCheck, ctx);
                }

                switch (shape.Relate(boundary))
                {
                    case Spatial4n.Shapes.SpatialRelation.Contains:
                        //This is termmatch.
                        yield return (geohashToCheck, true);
                        break;
                    case Spatial4n.Shapes.SpatialRelation.Within:
                    case Spatial4n.Shapes.SpatialRelation.Intersects:
                        using (var _ = Slice.From(allocator, geohashToCheck, out var term))
                        {
                            var amount = searcher.NumberOfDocumentsUnderSpecificTerm(tree, term);
                            if (amount == 0)
                            {
                                continue;
                            }

                            if (amount <= Threshold)
                            {
                                yield return (geohashToCheck, false);
                            }
                            else
                            {
                                var innerGeohashes = GetGeohashesForQueriesInsideShape(searcher, tree, allocator, ctx, shape, currentPrecision + 1, geohashToCheck,
                                    maxPrecision);

                                foreach (var geohash in innerGeohashes)
                                {
                                    yield return geohash;
                                }
                            }
                        }

                        continue;
                    default:
                        continue;
                }
            }
        }
    }
}
