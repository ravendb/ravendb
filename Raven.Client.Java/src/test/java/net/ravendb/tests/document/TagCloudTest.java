package net.ravendb.tests.document;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class TagCloudTest extends RemoteClientTest {

  @Test
  public void canQueryMapReduceIndex() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from post in docs.Posts " +
          " from Tag in post.Tags " +
          " select new { Tag, Count = 1 }");
      indexDefinition.setReduce("from result in results " +
          "group result by result.Tag into g " +
          "select new { Tag = g.Key, Count = g.Sum(x => (long)x.Count) }");
      indexDefinition.getIndexes().put("Tag", FieldIndexing.NOT_ANALYZED);

      store.getDatabaseCommands().putIndex("TagCloud", indexDefinition);

      try (IDocumentSession session = store.openSession()) {
        Post post1 = new Post();
        post1.setPostedAt(new Date());
        post1.setTags(Arrays.asList("C#", "Programming", "NoSql"));
        session.store(post1);

        Post post2 = new Post();
        post2.setPostedAt(new Date());
        post2.setTags(Arrays.asList("Database", "NoSql"));
        session.store(post2);
        session.saveChanges();

        List<TagAndCount> tagAndCounts = session.advanced().documentQuery(TagAndCount.class, "TagCloud").waitForNonStaleResults().toList();

        Map<String, Long> countMap = new HashMap<>();
        for (TagAndCount tac : tagAndCounts) {
          countMap.put(tac.getTag(), tac.getCount());
        }

        assertEquals(Long.valueOf(1), countMap.get("C#"));
        assertEquals(Long.valueOf(1), countMap.get("Database"));
        assertEquals(Long.valueOf(2), countMap.get("NoSql"));
        assertEquals(Long.valueOf(1), countMap.get("Programming"));

      }
    }
  }

  @Test
  public void canStoreAndRetrieveTime() throws Exception {
    Date expectedTime = new Date();

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Post post = new Post();
        post.setPostedAt(expectedTime);
        post.setTags(Arrays.asList("C#", "Programming", "NoSql"));
        session.store(post);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        List<Post> posts = session.query(Post.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResultsAsOfNow(5 * 1000)).toList();
        assertEquals(1, posts.size());
        assertEquals(expectedTime, posts.get(0).getPostedAt());
      }
    }
  }

  @Test
  public void canQueryMapReduceIndex_WithUpdates() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from post in docs.Posts " +
          " from Tag in post.Tags " +
          " select new { Tag, Count = 1 }");
      indexDefinition.setReduce("from result in results " +
          "group result by result.Tag into g " +
          "select new { Tag = g.Key, Count = g.Sum(x => (long)x.Count) }");
      indexDefinition.getIndexes().put("Tag", FieldIndexing.NOT_ANALYZED);

      store.getDatabaseCommands().putIndex("TagCloud", indexDefinition);

      try (IDocumentSession session = store.openSession()) {
        Post post1 = new Post();
        post1.setPostedAt(new Date());
        post1.setTags(Arrays.asList("C#", "Programming", "NoSql"));
        session.store(post1);

        Post post2 = new Post();
        post2.setPostedAt(new Date());
        post2.setTags(Arrays.asList("Database", "NoSql"));
        session.store(post2);
        session.saveChanges();

        List<TagAndCount> tagAndCounts = session.advanced().documentQuery(TagAndCount.class, "TagCloud").waitForNonStaleResults().toList();

        Map<String, Long> countMap = new HashMap<>();
        for (TagAndCount tac : tagAndCounts) {
          countMap.put(tac.getTag(), tac.getCount());
        }

        assertEquals(Long.valueOf(1), countMap.get("C#"));
        assertEquals(Long.valueOf(1), countMap.get("Database"));
        assertEquals(Long.valueOf(2), countMap.get("NoSql"));
        assertEquals(Long.valueOf(1), countMap.get("Programming"));

        Post post3 = new Post();
        post3.setPostedAt(new Date());
        post3.setTags(Arrays.asList("C#", "Programming", "NoSql"));
        session.store(post3);

        Post post4 = new Post();
        post4.setPostedAt(new Date());
        post4.setTags(Arrays.asList("Database", "NoSql"));
        session.store(post4);
        session.saveChanges();

        tagAndCounts = session.advanced().documentQuery(TagAndCount.class, "TagCloud").waitForNonStaleResults().toList();

        countMap = new HashMap<>();
        for (TagAndCount tac : tagAndCounts) {
          countMap.put(tac.getTag(), tac.getCount());
        }

        assertEquals(Long.valueOf(2), countMap.get("C#"));
        assertEquals(Long.valueOf(2), countMap.get("Database"));
        assertEquals(Long.valueOf(4), countMap.get("NoSql"));
        assertEquals(Long.valueOf(2), countMap.get("Programming"));

      }
    }
  }

  @Test
  public void canQueryMapReduceIndexOnMultipleFields() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from doc in docs " +
          " where doc.Activity != null " +
          " select new { " +
          "   Activity = doc.Activity, " +
          "   Character = doc.Character, " +
          "   Amount = doc.Amount " +
          " }");
      indexDefinition.setReduce("from result in results " +
          " group result by new { result.Activity, result.Character } into g " +
          " select new " +
          "     { " +
          "      Activity = g.Key.Activity, " +
          "       Character =  g.Key.Character, " +
          "       Amount = g.Sum(x=>(long)x.Amount) " +
          "     }");
      indexDefinition.getIndexes().put("Activity", FieldIndexing.NOT_ANALYZED);
      indexDefinition.getIndexes().put("Character", FieldIndexing.NOT_ANALYZED);

      store.getDatabaseCommands().putIndex("EventsByActivityAndCharacterCountAmount", indexDefinition);

      try (IDocumentSession session = store.openSession()) {
        Event event1 = new Event();
        event1.setActivity("Reading");
        event1.setCharacter("Elf");
        event1.setAmount(5);
        session.store(event1);

        Event event2 = new Event();
        event2.setActivity("Reading");
        event2.setCharacter("Dwarf");
        event2.setAmount(7);
        session.store(event2);

        Event event3 = new Event();
        event3.setActivity("Reading");
        event3.setCharacter("Elf");
        event3.setAmount(10);
        session.store(event3);

        session.saveChanges();

        List<ActivityAndCharacterCountAmount> tagAndCounts = session.advanced().documentQuery(ActivityAndCharacterCountAmount.class, "EventsByActivityAndCharacterCountAmount")
          .waitForNonStaleResults(3600 * 1000).toList();

        assertEquals(2, tagAndCounts.size());
        for (ActivityAndCharacterCountAmount tagAndCount: tagAndCounts) {
          if (tagAndCount.getActivity().equals("Reading") && "Elf".equals(tagAndCount.getCharacter())) {
            assertEquals(15, tagAndCount.getAmount());
          } else if (tagAndCount.getActivity().equals("Reading") && "Dwarf".equals(tagAndCount.getCharacter())) {
            assertEquals(7, tagAndCount.getAmount());
          } else  {
            throw new IllegalStateException();
          }
        }
      }
    }
  }


  public static class ActivityAndCharacterCountAmount {
    private String activity;
    private String character;
    private long amount;
    public String getActivity() {
      return activity;
    }
    public void setActivity(String activity) {
      this.activity = activity;
    }
    public String getCharacter() {
      return character;
    }
    public void setCharacter(String character) {
      this.character = character;
    }
    public long getAmount() {
      return amount;
    }
    public void setAmount(long amount) {
      this.amount = amount;
    }

  }
  public static class TagAndCount {
    private String tag;
    private long count;
    public String getTag() {
      return tag;
    }
    public void setTag(String tag) {
      this.tag = tag;
    }
    public long getCount() {
      return count;
    }
    public void setCount(long count) {
      this.count = count;
    }
    @Override
    public String toString() {
      return "Tag: " + tag + ", Count: " + count;
    }
  }

  public static class Post {
    private String id;
    private String title;
    private Date postedAt;
    private List<String> tags;
    private String content;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getTitle() {
      return title;
    }
    public void setTitle(String title) {
      this.title = title;
    }
    public Date getPostedAt() {
      return postedAt;
    }
    public void setPostedAt(Date postedAt) {
      this.postedAt = postedAt;
    }
    public List<String> getTags() {
      return tags;
    }
    public void setTags(List<String> tags) {
      this.tags = tags;
    }
    public String getContent() {
      return content;
    }
    public void setContent(String content) {
      this.content = content;
    }

  }

  public static class Event {
    private String id;
    private String activity;
    private String character;
    private long amount;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getActivity() {
      return activity;
    }
    public void setActivity(String activity) {
      this.activity = activity;
    }
    public String getCharacter() {
      return character;
    }
    public void setCharacter(String character) {
      this.character = character;
    }
    public long getAmount() {
      return amount;
    }
    public void setAmount(long amount) {
      this.amount = amount;
    }

  }
}
