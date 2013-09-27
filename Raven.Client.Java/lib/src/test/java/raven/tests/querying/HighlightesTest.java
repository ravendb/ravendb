package raven.tests.querying;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;

import raven.abstractions.basic.Reference;
import raven.abstractions.indexing.FieldIndexing;
import raven.abstractions.indexing.FieldStorage;
import raven.abstractions.indexing.FieldTermVector;
import raven.client.FieldHighlightings;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.client.indexes.AbstractMultiMapIndexCreationTask;

public class HighlightesTest extends RemoteClientTest {

  public static interface ISearchable {
    public void setSlug(String slug);
    public void setTitle(String title);
    public void setContent(String content);
    public String getSlug();
    public String getTitle();
    public String getContent();
  }

  @QueryEntity
  public static class EventsItem implements ISearchable  {
    private int id;
    private String title;
    private String slug;
    private String content;
    public int getId() {
      return id;
    }
    public void setId(int id) {
      this.id = id;
    }
    @Override
    public String getTitle() {
      return title;
    }
    @Override
    public void setTitle(String title) {
      this.title = title;
    }
    @Override
    public String getSlug() {
      return slug;
    }
    @Override
    public void setSlug(String slug) {
      this.slug = slug;
    }
    @Override
    public String getContent() {
      return content;
    }
    @Override
    public void setContent(String content) {
      this.content = content;
    }
  }

  public static class SearchResults {
    private ISearchable result;
    private List<String> highlights;
    private String title;
    public ISearchable getResult() {
      return result;
    }
    public void setResult(ISearchable result) {
      this.result = result;
    }
    public List<String> getHighlights() {
      return highlights;
    }
    public void setHighlights(List<String> highlights) {
      this.highlights = highlights;
    }
    public String getTitle() {
      return title;
    }
    public void setTitle(String title) {
      this.title = title;
    }
  }

  public static class ContentSearchIndex extends AbstractMultiMapIndexCreationTask {
    public ContentSearchIndex() {
      addMap("from doc in docs.EventsItems let slug = doc.Id.ToString().Substring(doc.Id.ToString().IndexOf('/') + 1) " +
          " select new { Slug = slug, doc.Title, doc.Content }");

      QHighlightesTest_EventsItem x = QHighlightesTest_EventsItem.eventsItem;

      index(x.slug, FieldIndexing.ANALYZED);
      store(x.slug, FieldStorage.YES);
      termVector(x.slug, FieldTermVector.WITH_POSITIONS_AND_OFFSETS);

      index(x.title, FieldIndexing.ANALYZED);
      store(x.title, FieldStorage.YES);
      termVector(x.title, FieldTermVector.WITH_POSITIONS_AND_OFFSETS);

      index(x.content, FieldIndexing.ANALYZED);
      store(x.content, FieldStorage.YES);
      termVector(x.content, FieldTermVector.WITH_POSITIONS_AND_OFFSETS);

    }
  }

  @Test
  public void searchWithHighlightes() throws Exception {
    String q = "session";
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        EventsItem e = new EventsItem();
        e.setSlug("ravendb-indexes-explained");
        e.setTitle("RavenDB indexes explained");
        e.setContent("Itamar Syn-Hershko: Afraid of Map/Reduce? In this session, core RavenDB developer Itamar Syn-Hershko will walk through the RavenDB indexing process, grok it and much more.");
        session.store(e);
        session.saveChanges();
      }

      new ContentSearchIndex().execute(store);

      try (IDocumentSession session = store.openSession()) {
        Reference<FieldHighlightings> titleHighlighting = new Reference<>();
        Reference<FieldHighlightings> slugHighlighting = new Reference<>();
        Reference<FieldHighlightings> contentHighlighting = new Reference<>();

        List<ISearchable> results = session.advanced().luceneQuery(ISearchable.class, "ContentSearchIndex")
          .waitForNonStaleResultsAsOfNow()
          .highlight("Title", 128, 2, titleHighlighting)
          .highlight("Slug", 128, 2, slugHighlighting)
          .highlight("Content", 128, 2, contentHighlighting)
          .setHighlighterTags("<span style='background: yellow'>", " ")
          .search("Slug", q).boost(15.0)
          .search("Title", q).boost(12.0)
          .search("Content", q)
          .toList();


        for (ISearchable searchable : results) {
          String docId = session.advanced().getDocumentId(searchable);

          List<String> highlights = new ArrayList<>();
          @SuppressWarnings("unused")
          String title = null;
          String[] titles = titleHighlighting.value.getFragments(docId);
          if (titles.length == 1) {
            title = titles[0];
          } else {
            highlights.addAll(Arrays.asList(titleHighlighting.value.getFragments(docId)));
          }
          highlights.addAll(Arrays.asList(slugHighlighting.value.getFragments(docId)));
          highlights.addAll(Arrays.asList(contentHighlighting.value.getFragments(docId)));
        }


      }
    }
  }


}
