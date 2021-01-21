using System;
using GeoAPI;
using Lucene.Net.Documents;
using Lucene.Net.Spatial;
using Lucene.Net.Spatial.Prefix.Tree;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Sparrow.Json;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Distance;
using Spatial4n.Core.Shapes;

namespace Raven.Server.Documents.Indexes.Static.Spatial
{
    public class SpatialField
    {
        private static readonly NtsSpatialContext GeoContext;

        public readonly SpatialStrategy Strategy;

        private readonly SpatialOptions _options;
        private readonly NtsSpatialContext _ntsContext;
        private readonly ShapeStringReadWriter _shapeStringReadWriter;

        public SpatialUnits Units => _options.Units;

        static SpatialField()
        {
            GeometryServiceProvider.Instance = new NtsGeometryServices();
            GeoContext = new NtsSpatialContext(new NtsSpatialContextFactory { geo = true });
        }

        public SpatialField(string fieldName, SpatialOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _ntsContext = CreateNtsContext(options);
            _shapeStringReadWriter = new ShapeStringReadWriter(options, _ntsContext);
            Strategy = CreateStrategy(fieldName, options, _ntsContext);
        }

        private static NtsSpatialContext CreateNtsContext(SpatialOptions options)
        {
            if (options.Type == SpatialFieldType.Cartesian)
            {
                var nts = new NtsSpatialContext(new NtsSpatialContextFactory { geo = false, distCalc = new CartesianDistCalc(), worldBounds = null });
                nts.WorldBounds.Reset(options.MinX, options.MaxX, options.MinY, options.MaxY);
                return nts;
            }
            return GeoContext;
        }

        public NtsSpatialContext GetContext()
        {
            return _ntsContext;
        }

        private static SpatialStrategy CreateStrategy(string fieldName, SpatialOptions opt, NtsSpatialContext context)
        {
            switch (opt.Strategy)
            {
                case SpatialSearchStrategy.GeohashPrefixTree:
                    return new RecursivePrefixTreeStrategyThatSupportsWithin(new GeohashPrefixTree(context, opt.MaxTreeLevel), fieldName);
                case SpatialSearchStrategy.QuadPrefixTree:
                    return new RecursivePrefixTreeStrategyThatSupportsWithin(new QuadPrefixTree(context, opt.MaxTreeLevel), fieldName);
                case SpatialSearchStrategy.BoundingBox:
                    return new BBoxStrategyThatSupportsAllShapes(context, fieldName);
            }

            return null;
        }

        public AbstractField[] CreateIndexableFields(object value)
        {
            var shape = value as IShape;
            if (shape != null || TryReadShape(value, out shape))
            {
                var fields = Strategy.CreateIndexableFields(shape);
                Array.Resize(ref fields, fields.Length + 1);
                fields[fields.Length - 1] = new Field(Constants.Documents.Indexing.Fields.SpatialShapeFieldName, WriteShape(shape), Field.Store.YES, Field.Index.NO);

                return fields;
            }

            return Array.Empty<AbstractField>();
        }

        private bool TryReadShape(object value, out IShape shape)
        {
            if (value is LazyStringValue lsv)
            {
                shape = ReadShape(lsv.ToString());
                return true;
            }

            if (value is LazyCompressedStringValue lcsv)
            {
                shape = ReadShape(lcsv.ToString());
                return true;
            }

            if (value is string s)
            {
                shape = ReadShape(s);
                return true;
            }

            shape = default(IShape);
            return false;
        }

        public IShape ReadShape(string shapeWkt, SpatialUnits? unitOverride = null)
        {
            return _shapeStringReadWriter.ReadShape(shapeWkt, unitOverride);
        }

        private string WriteShape(IShape shape)
        {
            return _shapeStringReadWriter.WriteShape(shape);
        }

        public IShape ReadCircle(double radius, double latitude, double longitude, SpatialUnits? unitOverride)
        {
            var context = GetContext();

            if (_options.Type == SpatialFieldType.Geography)
                radius = ShapeStringReadWriter.TranslateCircleRadius(radius, unitOverride ?? _options.Units);

            return context.MakeCircle(longitude, latitude, radius);
        }

        public IShape ReadPoint(double latitude, double longitude)
        {
            return GetContext().MakePoint(longitude, latitude);
        }
    }
}
