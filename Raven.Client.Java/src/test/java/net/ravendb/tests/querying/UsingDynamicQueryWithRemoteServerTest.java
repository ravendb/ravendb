package net.ravendb.tests.querying;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.FieldStorage;
import net.ravendb.abstractions.indexing.FieldTermVector;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.FieldHighlightings;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.querying.QUsingDynamicQueryWithRemoteServerTest_Blog;
import net.ravendb.tests.querying.QUsingDynamicQueryWithRemoteServerTest_BlogWithHighlightResults;
import net.ravendb.tests.querying.QUsingDynamicQueryWithRemoteServerTest_Tag;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class UsingDynamicQueryWithRemoteServerTest extends RemoteClientTest {

  @Test
  public void canPerformDynamicQueryUsingClientLinqQuery() throws Exception {
    Blog blogOne = new Blog();
    blogOne.setTitle("one");
    blogOne.setCategory("Ravens");

    Blog blogTwo = new Blog();
    blogTwo.setTitle("two");
    blogTwo.setCategory("Rhinos");

    Blog blogThree = new Blog();
    blogThree.setTitle("three");
    blogThree.setCategory("Rhinos");

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.store(blogTwo);
        session.store(blogThree);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {

        QUsingDynamicQueryWithRemoteServerTest_Blog x = QUsingDynamicQueryWithRemoteServerTest_Blog.blog;
        List<Blog> results = session.query(Blog.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResultsAsOfNow())
            .where(x.category.eq("Rhinos").and(x.title.length().eq(3)))
            .toList();

        @SuppressWarnings("unused")
        List<Blog> blogs = session.advanced().luceneQuery(Blog.class)
        .where("Category:Rhinos AND Title.Length:3")
        .toList();

        assertEquals(1, results.size());
        assertEquals("two", results.get(0).getTitle());
        assertEquals("Rhinos", results.get(0).getCategory());
      }
    }
  }

  @Test
  public void canPerformDynamicQueryUsingClientLuceneQuery() throws Exception {

    Blog blogOne = new Blog();
    blogOne.setTitle("one");
    blogOne.setCategory("Ravens");

    Blog blogTwo = new Blog();
    blogTwo.setTitle("two");
    blogTwo.setCategory("Rhinos");

    Blog blogThree = new Blog();
    blogThree.setTitle("three");
    blogThree.setCategory("Rhinos");

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.store(blogTwo);
        session.store(blogThree);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        List<Blog> results = session.advanced().luceneQuery(Blog.class)
            .where("Title.Length:3 AND Category:Rhinos")
            .waitForNonStaleResultsAsOfNow().toList();

        assertEquals(1, results.size());
        assertEquals("two", results.get(0).getTitle());
        assertEquals("Rhinos", results.get(0).getCategory());
      }
    }
  }

  @Test
  public void canPerformProjectionUsingClientLinqQuery() throws Exception {
    Blog blogOne = new Blog();
    blogOne.setTitle("one");
    blogOne.setCategory("Ravens");
    blogOne.setTags(Arrays.asList(new Tag("tagOne"), new Tag("tagTwo")));


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QUsingDynamicQueryWithRemoteServerTest_Blog x = QUsingDynamicQueryWithRemoteServerTest_Blog.blog;
        QUsingDynamicQueryWithRemoteServerTest_Tag t = QUsingDynamicQueryWithRemoteServerTest_Tag.tag;
        Blog results = session.query(Blog.class)
            .where(x.title.eq("one").and(x.tags.any(t.name.eq("tagTwo"))))
            .select(Blog.class, x.category, x.title)
            .single();

        assertEquals("one", results.getTitle());
        assertEquals("Ravens", results.getCategory());
      }
    }
  }

  @Test
  public void queryForASpecificTypeDoesNotBringBackOtherTypes() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new Tag());
        session.saveChanges();
      }

      try(IDocumentSession session = store.openSession()) {
        QUsingDynamicQueryWithRemoteServerTest_Blog x = QUsingDynamicQueryWithRemoteServerTest_Blog.blog;
        List<String> results = session.query(Blog.class).select(x.category).toList();

        assertEquals(0, results.size());
      }
    }
  }

  @Test
  public void canPerformLinqOrderByOnNumericField() throws Exception {
    Blog blogOne = new Blog();
    blogOne.setSortWeight(2);

    Blog blogTwo = new Blog();
    blogTwo.setSortWeight(4);

    Blog blogThree = new Blog();
    blogThree.setSortWeight(1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.store(blogTwo);
        session.store(blogThree);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QUsingDynamicQueryWithRemoteServerTest_Blog x = QUsingDynamicQueryWithRemoteServerTest_Blog.blog;
        List<Blog> resultDescending = session.query(Blog.class)
            .orderBy(x.sortWeight.desc()).toList();

        List<Blog> resultAscending = session.query(Blog.class)
            .orderBy(x.sortWeight.asc()).toList();

        assertEquals(4, resultDescending.get(0).getSortWeight());
        assertEquals(2, resultDescending.get(1).getSortWeight());
        assertEquals(1, resultDescending.get(2).getSortWeight());

        assertEquals(1, resultAscending.get(0).getSortWeight());
        assertEquals(2, resultAscending.get(1).getSortWeight());
        assertEquals(4, resultAscending.get(2).getSortWeight());

      }
    }
  }

  @Test
  public void canPerformLinqOrderByOnTextField() throws Exception {
    Blog blogOne = new Blog();
    blogOne.setTitle("aaaaa");

    Blog blogTwo = new Blog();
    blogTwo.setTitle("ccccc");

    Blog blogThree = new Blog();
    blogThree.setTitle("bbbbb");

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.store(blogTwo);
        session.store(blogThree);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QUsingDynamicQueryWithRemoteServerTest_Blog x = QUsingDynamicQueryWithRemoteServerTest_Blog.blog;
        List<Blog> resultDescending = session.query(Blog.class).orderBy(x.title.desc()).toList();
        List<Blog> resultAscending = session.query(Blog.class).orderBy(x.title.asc()).toList();

        assertEquals("ccccc", resultDescending.get(0).getTitle());
        assertEquals("bbbbb", resultDescending.get(1).getTitle());
        assertEquals("aaaaa", resultDescending.get(2).getTitle());

        assertEquals("aaaaa", resultAscending.get(0).getTitle());
        assertEquals("bbbbb", resultAscending.get(1).getTitle());
        assertEquals("ccccc", resultAscending.get(2).getTitle());
      }
    }
  }

  @Test
  public void canPerformDynamicQueryWithHighlightingUsingClientLuceneQuery() throws Exception {
    Blog blogOne = new Blog();
    blogOne.setTitle("Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.");
    blogOne.setCategory("Ravens");

    Blog blogTwo = new Blog();
    blogTwo.setTitle("Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.");
    blogTwo.setCategory("The Rhinos");

    Blog blogThree = new Blog();
    blogThree.setTitle("Target cras vitae felis arcu word.");
    blogThree.setCategory("Los Rhinos");

    String blogOneId;
    String blogTwoId;

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.store(blogTwo);
        session.store(blogThree);
        session.saveChanges();

        blogOneId = session.advanced().getDocumentId(blogOne);
        blogTwoId = session.advanced().getDocumentId(blogTwo);
      }

      try (IDocumentSession session = store.openSession()) {
        Reference<FieldHighlightings> titleHighlightings = new Reference<>();
        Reference<FieldHighlightings> categoryHighlightings = new Reference<>();

        List<Blog> results = session.advanced().luceneQuery(Blog.class)
            .highlight("Title", 18, 2, titleHighlightings)
            .highlight("Category", 18, 2, categoryHighlightings)
            .setHighlighterTags("*", "*")
            .where("Title:(target word) OR Category:rhinos")
            .waitForNonStaleResultsAsOfNow()
            .toList();

        assertEquals(3, results.size());
        assertTrue(titleHighlightings.value.getFragments(blogOneId).length > 0);
        assertTrue(categoryHighlightings.value.getFragments(blogOneId).length == 0);

        assertTrue(titleHighlightings.value.getFragments(blogTwoId).length > 0);
        assertTrue(categoryHighlightings.value.getFragments(blogTwoId).length > 0);
      }
    }
  }

  @Test
  public void canPerformDynamicQueryWithHighlighting() throws Exception {
    Blog blogOne = new Blog();
    blogOne.setTitle("Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.");
    blogOne.setCategory("Ravens");

    Blog blogTwo = new Blog();
    blogTwo.setTitle("Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.");
    blogTwo.setCategory("The Rhinos");

    Blog blogThree = new Blog();
    blogThree.setTitle("Target cras vitae felis arcu word.");
    blogThree.setCategory("Los Rhinos");

    String blogOneId;
    String blogTwoId;

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.store(blogTwo);
        session.store(blogThree);
        session.saveChanges();

        blogOneId = session.advanced().getDocumentId(blogOne);
        blogTwoId = session.advanced().getDocumentId(blogTwo);
      }

      try (IDocumentSession session = store.openSession()) {
        Reference<FieldHighlightings> titleHighlightings = new Reference<>();
        Reference<FieldHighlightings> categoryHighlightings = new Reference<>();

        QUsingDynamicQueryWithRemoteServerTest_Blog x = QUsingDynamicQueryWithRemoteServerTest_Blog.blog;

        List<Blog> results = session.query(Blog.class)
            .customize(new DocumentQueryCustomizationFactory()
            .highlight("Title", 18, 2, titleHighlightings)
            .highlight("Category", 18, 2, categoryHighlightings)
            .setHighlighterTags("*", "*")
            .waitForNonStaleResultsAsOfNow())
            .search(x.category, "rhinos")
            .search(x.title, "target word")
            .toList();


        assertEquals(3, results.size());
        assertTrue(titleHighlightings.value.getFragments(blogOneId).length > 0);
        assertTrue(categoryHighlightings.value.getFragments(blogOneId).length == 0);

        assertTrue(titleHighlightings.value.getFragments(blogTwoId).length > 0);
        assertTrue(categoryHighlightings.value.getFragments(blogTwoId).length > 0);
      }
    }
  }

  @Test
  public void executesQueryWithHighlightingsAgainstSimpleIndex() throws Exception {
    String indexName = "BlogsForHighlightingTests";
    IndexDefinition indexDefinition = new IndexDefinition();
    indexDefinition.setMap("from blog in docs.Blogs select new { blog.Title, blog.Category }");
    indexDefinition.getStores().put("Title", FieldStorage.YES);
    indexDefinition.getStores().put("Category", FieldStorage.YES);
    indexDefinition.getIndexes().put("Title", FieldIndexing.ANALYZED);
    indexDefinition.getIndexes().put("Category", FieldIndexing.ANALYZED);
    indexDefinition.getTermVectors().put("Title", FieldTermVector.WITH_POSITIONS_AND_OFFSETS);
    indexDefinition.getTermVectors().put("Category", FieldTermVector.WITH_POSITIONS_AND_OFFSETS);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().putIndex(indexName, indexDefinition);

      Blog blogOne = new Blog();
      blogOne.setTitle("Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.");
      blogOne.setCategory("Ravens");

      Blog blogTwo = new Blog();
      blogTwo.setTitle("Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.");
      blogTwo.setCategory("The Rhinos");

      Blog blogThree = new Blog();
      blogThree.setTitle("Target cras vitae felis arcu word.");
      blogThree.setCategory("Los Rhinos");

      String blogOneId;
      String blogTwoId;

      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.store(blogTwo);
        session.store(blogThree);
        session.saveChanges();

        blogOneId = session.advanced().getDocumentId(blogOne);
        blogTwoId = session.advanced().getDocumentId(blogTwo);
      }

      try (IDocumentSession session = store.openSession()) {
        Reference<FieldHighlightings> titleHighlightings = new Reference<>();
        Reference<FieldHighlightings> categoryHighlightings = new Reference<>();

        QUsingDynamicQueryWithRemoteServerTest_Blog x = QUsingDynamicQueryWithRemoteServerTest_Blog.blog;

        List<Blog> results = session.query(Blog.class, indexName)
            .customize(new DocumentQueryCustomizationFactory()
            .highlight("Title", 18, 2, titleHighlightings)
            .highlight("Category", 18, 2, categoryHighlightings)
            .setHighlighterTags("*", "*")
            .waitForNonStaleResultsAsOfNow())
            .search(x.category, "rhinos")
            .search(x.title, "target word")
            .toList();


        assertEquals(3, results.size());
        assertTrue(titleHighlightings.value.getFragments(blogOneId).length > 0);
        assertTrue(categoryHighlightings.value.getFragments(blogOneId).length == 0);

        assertTrue(titleHighlightings.value.getFragments(blogTwoId).length > 0);
        assertTrue(categoryHighlightings.value.getFragments(blogTwoId).length > 0);
      }
    }
  }

  @Test
  public void executesQueryWithHighlightingsAndProjections() throws Exception {
    String indexName = "BlogsForHighlightingTests";
    IndexDefinition indexDefinition = new IndexDefinition();
    indexDefinition.setMap("from blog in docs.Blogs select new { blog.Title, blog.Category }");
    indexDefinition.getStores().put("Title", FieldStorage.YES);
    indexDefinition.getStores().put("Category", FieldStorage.YES);
    indexDefinition.getIndexes().put("Title", FieldIndexing.ANALYZED);
    indexDefinition.getIndexes().put("Category", FieldIndexing.ANALYZED);
    indexDefinition.getTermVectors().put("Title", FieldTermVector.WITH_POSITIONS_AND_OFFSETS);
    indexDefinition.getTermVectors().put("Category", FieldTermVector.WITH_POSITIONS_AND_OFFSETS);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().putIndex(indexName, indexDefinition);

      Blog blogOne = new Blog();
      blogOne.setTitle("Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.");
      blogOne.setCategory("Ravens");

      Blog blogTwo = new Blog();
      blogTwo.setTitle("Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.");
      blogTwo.setCategory("The Rhinos");

      Blog blogThree = new Blog();
      blogThree.setTitle("Target cras vitae felis arcu word.");
      blogThree.setCategory("Los Rhinos");

      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.store(blogTwo);
        session.store(blogThree);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {

        QUsingDynamicQueryWithRemoteServerTest_Blog x = QUsingDynamicQueryWithRemoteServerTest_Blog.blog;
        QUsingDynamicQueryWithRemoteServerTest_BlogWithHighlightResults r = QUsingDynamicQueryWithRemoteServerTest_BlogWithHighlightResults.blogWithHighlightResults;

        List<BlogWithHighlightResults> results = session.query(Blog.class, indexName)
            .customize(new DocumentQueryCustomizationFactory()
            .waitForNonStaleResults()
            .highlight("Title", 18, 2, "TitleFragments"))
            .where(x.title.eq("lorem").and(x.category.eq("ravens")))
            .select(BlogWithHighlightResults.class, r.title, r.category, r.titleFragments)
            .toList();

        assertEquals(1, results.size());
        assertTrue(results.get(0).getTitleFragments().length > 0);
      }
    }
  }

  @Test
  public void executesQueryWithHighlightingsAgainstMapReduceIndex() throws Exception {
    String indexName = "BlogsForHighlightingMRTests";
    IndexDefinition indexDefinition = new IndexDefinition();
    indexDefinition.setMap("from blog in docs.Blogs select new { blog.Title, blog.Category }");
    indexDefinition.setReduce("from result in results " +
        "group result by result.Category into g " +
        "select new { Category = g.Key, Title = g.Select(x=>x.Title).Aggregate(string.Concat) }");
    indexDefinition.getStores().put("Title", FieldStorage.YES);
    indexDefinition.getStores().put("Category", FieldStorage.YES);
    indexDefinition.getIndexes().put("Title", FieldIndexing.ANALYZED);
    indexDefinition.getIndexes().put("Category", FieldIndexing.ANALYZED);
    indexDefinition.getTermVectors().put("Title", FieldTermVector.WITH_POSITIONS_AND_OFFSETS);
    indexDefinition.getTermVectors().put("Category", FieldTermVector.WITH_POSITIONS_AND_OFFSETS);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.getDatabaseCommands().putIndex(indexName, indexDefinition);
      Blog blogOne = new Blog();
      blogOne.setTitle("Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.");
      blogOne.setCategory("Ravens");

      Blog blogTwo = new Blog();
      blogTwo.setTitle("Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.");
      blogTwo.setCategory("The Rhinos");

      Blog blogThree = new Blog();
      blogThree.setTitle("Target cras vitae felis arcu word.");
      blogThree.setCategory("Los Rhinos");

      try (IDocumentSession session = store.openSession()) {
        session.store(blogOne);
        session.store(blogTwo);
        session.store(blogThree);
        session.saveChanges();
      }


      try (IDocumentSession session = store.openSession()) {

        QUsingDynamicQueryWithRemoteServerTest_Blog x = QUsingDynamicQueryWithRemoteServerTest_Blog.blog;
        QUsingDynamicQueryWithRemoteServerTest_BlogWithHighlightResults r = QUsingDynamicQueryWithRemoteServerTest_BlogWithHighlightResults.blogWithHighlightResults;

        List<BlogWithHighlightResults> results = session.query(Blog.class, indexName)
            .customize(new DocumentQueryCustomizationFactory()
            .waitForNonStaleResults()
            .highlight("Title", 18, 2, "TitleFragments"))
            .where(x.title.eq("lorem").and(x.category.eq("ravens")))
            .select(BlogWithHighlightResults.class, r.title, r.category, r.titleFragments)
            .toList();

        assertEquals(1, results.size());
        assertTrue(results.get(0).getTitleFragments().length > 0);
      }
    }
  }

  @QueryEntity
  public static class BlogWithHighlightResults extends Blog {
    private String[] titleFragments;

    public String[] getTitleFragments() {
      return titleFragments;
    }

    public void setTitleFragments(String[] titleFragments) {
      this.titleFragments = titleFragments;
    }

  }

  @QueryEntity
  public static class Blog {
    private User user;
    private String title;
    private List<Tag> tags;
    private int sortWeight;
    private String category;
    public User getUser() {
      return user;
    }
    public void setUser(User user) {
      this.user = user;
    }
    public String getTitle() {
      return title;
    }
    public void setTitle(String title) {
      this.title = title;
    }
    public List<Tag> getTags() {
      return tags;
    }
    public void setTags(List<Tag> tags) {
      this.tags = tags;
    }
    public int getSortWeight() {
      return sortWeight;
    }
    public void setSortWeight(int sortWeight) {
      this.sortWeight = sortWeight;
    }
    public String getCategory() {
      return category;
    }
    public void setCategory(String category) {
      this.category = category;
    }

  }

  @QueryEntity
  public static class Tag {
    private String name;

    public Tag() {
      super();
    }

    public Tag(String name) {
      super();
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  @QueryEntity
  public static class User {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }
}
