using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Spatial.Prefix.Tree;
using Raven.Database.Indexing.Spatial;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Io;
using Spatial4n.Core.Shapes;
using Spatial4n.Core.Shapes.Nts;
using Spatial4n.Core.Util;
using System.Linq;

internal class Program
{
	private static void Main(string[] args)
	{
		var ctx = new NtsSpatialContext(true);
		var startegy = new RecursivePrefixTreeStrategyThatSupportsWithin(new GeohashPrefixTree(ctx, 10), "test");
		var poly = @"POLYGON ((-110.947642 55.333333, -109.644025 55.333333, -109.373584 55.253002, -109.132938 55.146591, -108.929565 55.017525, -108.769563 54.869919, -108.657484 54.708433, -108.596239 54.538106, -108.587078 54.364188, -108.629626 54.191974, -108.721976 54.026641, -108.860817 53.873103, -109.041589 53.735872, -109.258661 53.618939, -109.505505 53.525678, -109.77489 53.45876, -110.059063 53.420091, -110.34994 53.410769, -110.639287 53.431058, -110.918901 53.480383, -111.180801 53.557342, -111.417406 53.659739, -111.621724 53.784634, -111.787539 53.928413, -111.909595 54.086879, -111.983776 54.255356, -112.007276 54.428817, -111.978744 54.602021, -111.898399 54.769672, -111.768106 54.926578, -111.591391 55.067822, -111.373396 55.188926, -111.120753 55.286017, -110.947642 55.333333))";
		var shapeReaderWriter = new NtsShapeReadWriter(ctx);
		Shape shape = shapeReaderWriter.ReadShape(poly);
		var indexableFields = startegy.CreateIndexableFields(shape);
		var tokenStreamValue = indexableFields[0].TokenStreamValue;
		int c = 0;
		while (tokenStreamValue.IncrementToken())
		{
			var termAttribute = tokenStreamValue.GetAttribute<ITermAttribute>();
			if (termAttribute.Term.EndsWith("+") == false)
				continue;
			var boundary = GeohashUtils.DecodeBoundary(termAttribute.Term.Replace("+", ""), ctx);
			var s = ("POLYGON ((" + boundary.GetMinX() + " " + boundary.GetMinY()+"," + 
				boundary.GetMinX() + " " + boundary.GetMaxY() + ", " +
				boundary.GetMaxX() + " " + boundary.GetMaxY() + ", " +
				boundary.GetMaxX() + " " + boundary.GetMinY() + ", " +
				boundary.GetMinX() + " " + boundary.GetMinY() + "))");
			c++;
		}
		Console.WriteLine(c);
		var geometry = ((NtsGeometry) shape).GetGeom();
		var area = geometry.Area;
		
		Console.WriteLine(area);
		Console.WriteLine(ctx.GetDistCalc().Area(shape.GetBoundingBox()));
		//var points = geometry.Coordinates.Select(x => new PointF(T(x.X), T(x.Y))).ToArray();

		
	}

	private static float T(double p0)
	{
		return (float) (p0 + 180)*2;
	}
}

public class MyItem
{
	public string Id { get; set; }
	public string Hash { get; set; }
}