package net.ravendb.client.document;

import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialUnits;
import net.ravendb.client.FieldHighlightings;
import net.ravendb.client.IDocumentQueryCustomization;
import net.ravendb.client.spatial.SpatialCriteria;


import com.mysema.query.types.Path;

/**
 * Allows query customization
 */
@Immutable
public class DocumentQueryCustomizationFactory {

  private Set<Action1<IDocumentQueryCustomization>> actions = new LinkedHashSet<>();

  public DocumentQueryCustomizationFactory() {
    super();
  }

  protected DocumentQueryCustomizationFactory(Set<Action1<IDocumentQueryCustomization>> actions, Action1<IDocumentQueryCustomization> newAction) {
    this.actions.addAll(actions);
    this.actions.add(newAction);
  }

  /**
   * Instructs the query to wait for non stale results as of the last write made by any session belonging to the
   * current document store.
   * This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
   * However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results.
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResultsAsOfLastWrite() {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResultsAsOfLastWrite();
      }
    });
  }

  /**
   * Instructs the query to wait for non stale results as of the last write made by any session belonging to the
   * current document store.
   * This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
   * However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results.
   * @param waitTimeout
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResultsAsOfLastWrite(final long waitTimeout) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResultsAsOfLastWrite(waitTimeout);
      }
    });
  }

  /**
   * Instructs the query to wait for non stale results as of now.
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResultsAsOfNow() {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResultsAsOfNow();
      }
    });
  }

  /**
   * Instructs the query to wait for non stale results as of now for the specified timeout.
   * @param waitTimeout timeout in milis
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResultsAsOfNow(final long waitTimeout) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResultsAsOfNow(waitTimeout);
      }
    });
  }

  /**
   * Instructs the query to wait for non stale results as of the cutoff date.
   * @param cutOff
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResultsAsOf(final Date cutOff) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResultsAsOf(cutOff);
      }
    });
  }

  /**
   * Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
   * @param cutOff
   * @param waitTimeout timeout in milis
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResultsAsOf(final Date cutOff, final long waitTimeout) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResultsAsOf(cutOff, waitTimeout);
      }
    });
  }

  /**
   * Instructs the query to wait for non stale results as of the cutoff etag.
   * @param cutOffEtag
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResultsAsOf(final Etag cutOffEtag) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResultsAsOf(cutOffEtag);
      }
    });
  }

  /**
   * Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
   * @param cutOffEtag
   * @param waitTimeout
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResultsAsOf(final Etag cutOffEtag, final long waitTimeout) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResultsAsOf(cutOffEtag, waitTimeout);
      }
    });
  }

  /**
   * EXPERT ONLY: Instructs the query to wait for non stale results.
   * This shouldn't be used outside of unit tests unless you are well aware of the implications
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResults() {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResults();
      }
    });
  }

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param path
   * @return
   */
  public DocumentQueryCustomizationFactory include(final Path<?> path) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.include(path);
      }
    });
  }

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param path
   * @return
   */
  public DocumentQueryCustomizationFactory include(final String path) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.include(path);
      }
    });
  }

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param targetEntityClass
   * @param path
   * @return
   */
  public DocumentQueryCustomizationFactory include(final Class<?> targetEntityClass, final Path<?> path) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.include(targetEntityClass, path);
      }
    });
  }

  /**
   * EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
   * This shouldn't be used outside of unit tests unless you are well aware of the implications
   * @param waitTimeout
   * @return
   */
  public DocumentQueryCustomizationFactory waitForNonStaleResults(final long waitTimeout) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.waitForNonStaleResults(waitTimeout);
      }
    });
  }

  /**
   * Filter matches to be inside the specified radius
   * @param radius
   * @param latitude
   * @param longitude
   * @return
   */
  public DocumentQueryCustomizationFactory withinRadiusOf(final double radius, final double latitude, final double longitude) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.withinRadiusOf(radius, latitude, longitude);
      }
    });
  }

  /**
   * Filter matches to be inside the specified radius
   * @param fieldName
   * @param radius
   * @param latitude
   * @param longitude
   * @return
   */
  public DocumentQueryCustomizationFactory withinRadiusOf(final String fieldName, final double radius, final double latitude, final double longitude) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.withinRadiusOf(fieldName, radius, latitude, longitude);
      }
    });
  }

  /**
   * Filter matches to be inside the specified radius
   * @param radius
   * @param latitude
   * @param longitude
   * @param radiusUnits
   * @return
   */
  public DocumentQueryCustomizationFactory withinRadiusOf(final double radius, final double latitude, final double longitude, final SpatialUnits radiusUnits) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.withinRadiusOf(radius, latitude, longitude, radiusUnits);
      }
    });
  }

  /**
   * Filter matches to be inside the specified radius
   * @param fieldName
   * @param radius
   * @param latitude
   * @param longitude
   * @param radiusUnits
   * @return
   */
  public DocumentQueryCustomizationFactory withinRadiusOf(final String fieldName, final double radius, final double latitude, final double longitude, final SpatialUnits radiusUnits) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.withinRadiusOf(fieldName, radius, latitude, longitude, radiusUnits);
      }
    });
  }

  /**
   * Filter matches based on a given shape - only documents with the shape defined in fieldName that
   * have a relation rel with the given shapeWKT will be returned
   * @param fieldName The name of the field containing the shape to use for filtering
   * @param shapeWKT The query shape
   * @param rel Spatial relation to check
   * @return
   */
  public DocumentQueryCustomizationFactory relatesToShape(final String fieldName, final String shapeWKT, final SpatialRelation rel) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.relatesToShape(fieldName, shapeWKT, rel);
      }
    });
  }

  public DocumentQueryCustomizationFactory spatial(final String fieldName, final SpatialCriteria criteria) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.spatial(fieldName, criteria);
      }
    });
  }

  /**
   * When using spatial queries, instruct the query to sort by the distance from the origin point
   * @return
   */
  public DocumentQueryCustomizationFactory sortByDistance() {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.sortByDistance();
      }
    });
  }

  /**
   * Order the search results randomly
   * @return
   */
  public DocumentQueryCustomizationFactory randomOrdering() {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.randomOrdering();
      }
    });
  }

  /**
   * Order the search results randomly using the specified seed
   * this is useful if you want to have repeatable random queries
   * @param seed
   * @return
   */
  public DocumentQueryCustomizationFactory randomOrdering(final String seed) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.randomOrdering(seed);
      }
    });
  }

  /**
   * Allow you to modify the index query before it is executed
   * @param action
   * @return
   */
  public DocumentQueryCustomizationFactory beforeQueryExecution(final Action1<IndexQuery> action) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.beforeQueryExecution(action);
      }
    });
  }

  /**
   * Adds matches highlighting for the specified field.
   *
   * The specified field should be analysed and stored for highlighter to work.
   *  For each match it creates a fragment that contains matched text surrounded by highlighter tags.
   * @param fieldName The field name to highlight.
   * @param fragmentLength The fragment length.
   * @param fragmentCount The maximum number of fragments for the field.
   * @param fragmentsField The field in query results item to put highlightings into.
   * @return
   */
  public DocumentQueryCustomizationFactory highlight(final String fieldName, final int fragmentLength, final int fragmentCount, final String fragmentsField) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
      }
    });
  }

  /**
   * Adds matches highlighting for the specified field.
   *
   * The specified field should be analysed and stored for highlighter to work.
   * For each match it creates a fragment that contains matched text surrounded by highlighter tags.
   * @param fieldName The field name to highlight.
   * @param fragmentLength The fragment length.
   * @param fragmentCount The maximum number of fragments for the field.
   * @param highlightings Field highlightings for all results.
   * @return
   */
  public DocumentQueryCustomizationFactory highlight(final String fieldName, final int fragmentLength, final int fragmentCount, final Reference<FieldHighlightings> highlightings) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.highlight(fieldName, fragmentLength, fragmentCount, highlightings);
      }
    });
  }

  /**
   * Sets the tags to highlight matches with.
   * @param preTag Prefix tag.
   * @param postTag Postfix tag.
   * @return
   */
  public DocumentQueryCustomizationFactory setHighlighterTags(final String preTag, final String postTag) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.setHighlighterTags(preTag, postTag);
      }
    });
  }

  /**
   * Sets the tags to highlight matches with.
   * @param preTags Prefix tags.
   * @param postTags Postfix tags.
   * @return
   */
  public DocumentQueryCustomizationFactory setHighlighterTags(final String[] preTags, final String[] postTags) {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.setHighlighterTags(preTags, postTags);
      }
    });
  }

  /**
   * Disables tracking for queried entities by Raven's Unit of Work.
   * Usage of this option will prevent holding query results in memory.
   * @return
   */
  public DocumentQueryCustomizationFactory noTracking() {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.noTracking();
      }
    });
  }

  /**
   * Disables caching for query results.
   * @return
   */
  public DocumentQueryCustomizationFactory noCaching() {
    return new DocumentQueryCustomizationFactory(actions, new Action1<IDocumentQueryCustomization>() {
      @Override
      public void apply(IDocumentQueryCustomization documentQuery) {
        documentQuery.noCaching();
      }
    });
  }

  public void customize(IDocumentQueryCustomization documentQuery) {
    for (Action1<IDocumentQueryCustomization> action: actions) {
      action.apply(documentQuery);
    }
  }

  public static DocumentQueryCustomizationFactory join(DocumentQueryCustomizationFactory firstCustomize, DocumentQueryCustomizationFactory secondCustomize) {
    DocumentQueryCustomizationFactory mergedFactory = new DocumentQueryCustomizationFactory();
    mergedFactory.actions.addAll(firstCustomize.actions);
    mergedFactory.actions.addAll(secondCustomize.actions);
    return mergedFactory;
  }


}
