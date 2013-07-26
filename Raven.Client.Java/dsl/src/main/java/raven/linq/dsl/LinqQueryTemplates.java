package raven.linq.dsl;

import com.mysema.query.types.Ops;
import com.mysema.query.types.Templates;

public class LinqQueryTemplates extends Templates {

  public static final LinqQueryTemplates DEFAULT = new LinqQueryTemplates();

  public LinqQueryTemplates() {

    add(Ops.EQ, "{0} == {1}", 18);

    add(Ops.COL_SIZE, "{0}.Length");

    add(Ops.STARTS_WITH, "{0}.StartsWith({1})");
    add(Ops.ENDS_WITH, "{0}.EndsWith({1})");

    add(LinqOps.SUM, "{0}.Sum({1})");
    add(LinqOps.LAMBDA, "{0} => {1}");

    add(LinqOps.Markers.CREATE_FIELD2, "this.CreateField({0}, {1})");
    add(LinqOps.Markers.CREATE_FIELD4, "this.CreateField({0}, {1}, {2}, {3})");
    add(LinqOps.Markers.SPATIAL_GENERATE2, "AbstractIndexCreationTask.SpatialGenerate((double?) {1}, (double?) {2})");
    add(LinqOps.Markers.SPATIAL_GENERATE3, "AbstractIndexCreationTask.SpatialGenerate({0}, (double?) {1}, (double?) {2})");
    add(LinqOps.Markers.SPATIAL_INDEX_GENERATE2, "SpatialIndex.Generate((double?) {1}, (double?) {2})");
    add(LinqOps.Markers.SPATIAL_INDEX_GENERATE3, "SpatialIndex.Generate({0}, (double?) {1}, (double?) {2})");
    add(LinqOps.Markers.SPATIAL_CLUSTERING3, "this.SpatialClustering({0}, (double?) {1}, (double?) {2})");
    add(LinqOps.Markers.SPATIAL_CLUSTERING5, "this.SpatialClustering({0}, (double?) {1}, (double?) {2}, {3}, {4})");
    add(LinqOps.Markers.SPATIAL_WKT_GENERATE2, "AbstractIndexCreationTask.SpatialGenerate({0}, {1})");
    add(LinqOps.Markers.SPATIAL_WKT_GENERATE3, "AbstractIndexCreationTask.SpatialGenerate({0}, {1}, {2})");
    add(LinqOps.Markers.SPATIAL_WKT_GENERATE4, "AbstractIndexCreationTask.SpatialGenerate({0}, {1}, {2}, {3})");


    add(LinqOps.Fluent.GROUP_BY, "{0}.GroupBy({1})");
    add(LinqOps.Fluent.ORDER_BY, "{0}.OrderBy({1})");
    add(LinqOps.Fluent.ORDER_BY_DESC, "{0}.OrderByDescending({1})");
    add(LinqOps.Fluent.SELECT, "{0}.Select({1})");
    add(LinqOps.Fluent.SELECT_MANY_TRANSLATED, "{0}.SelectMany({1}, {2})");
    add(LinqOps.Fluent.WHERE, "{0}.Where({1})");


    //TODO: work on another templates + create super class for general .net templates
  }


}
