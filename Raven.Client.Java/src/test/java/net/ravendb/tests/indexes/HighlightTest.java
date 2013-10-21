package net.ravendb.tests.indexes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.FieldStorage;
import net.ravendb.abstractions.indexing.FieldTermVector;
import net.ravendb.client.FieldHighlightings;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.indexes.QHighlightTest_SearchItem;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class HighlightTest extends RemoteClientTest {

  @SuppressWarnings("unused")
  @Test
  public void highlightText() throws Exception {
    SearchItem searchItem = new SearchItem();
    searchItem.setId("searchitems/1");
    searchItem.setName("This is a sample about a dog and his owner");
    String searchFor = "about";


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(searchItem);
        store.getDatabaseCommands().putIndex(new ContentSearchIndex().getIndexName(), new ContentSearchIndex().createIndexDefinition());
        session.saveChanges();

        Reference<FieldHighlightings> nameHighlightingRef = new Reference<>();
        List<SearchItem> results = session.advanced().luceneQuery(SearchItem.class, "ContentSearchIndex")
            .waitForNonStaleResults()
            .highlight("Name", 128, 1, nameHighlightingRef)
            .search("Name", searchFor)
            .toList();

        assertTrue(nameHighlightingRef.value.getFragments("searchitems/1").length> 0);
        assertEquals("This is a sample <b style=\"background:yellow\">about</b> a dog and his owner", nameHighlightingRef.value.getFragments("searchitems/1")[0]);
      }
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void highlightText_CutAfterDot() throws Exception {
    SearchItem searchItem = new SearchItem();
    searchItem.setId("searchitems/1");
    searchItem.setName("This is a. sample about a dog and his owner");
    String searchFor = "about";

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(searchItem);
        store.getDatabaseCommands().putIndex(new ContentSearchIndex().getIndexName(), new ContentSearchIndex().createIndexDefinition());
        session.saveChanges();

        Reference<FieldHighlightings> nameHighlightingRef = new Reference<>();
        List<SearchItem> results = session.advanced().luceneQuery(SearchItem.class, "ContentSearchIndex")
            .waitForNonStaleResults(5 * 60 * 1000)
            .highlight("Name", 128, 1, nameHighlightingRef)
            .search("Name", searchFor)
            .toList();

        assertTrue(nameHighlightingRef.value.getFragments("searchitems/1").length> 0);
        assertEquals("sample <b style=\"background:yellow\">about</b> a dog and his owner", nameHighlightingRef.value.getFragments("searchitems/1")[0]);
      }
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void highlightText_LineRerturnedShorterThenOriginal() throws Exception {
    SearchItem searchItem = new SearchItem();
    searchItem.setId("searchitems/1");
    searchItem.setName("This is a sample about a dog and his owner");
    String searchFor = "about";


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(searchItem);
        store.getDatabaseCommands().putIndex(new ContentSearchIndex().getIndexName(), new ContentSearchIndex().createIndexDefinition());
        session.saveChanges();

        Reference<FieldHighlightings> nameHighlightingRef = new Reference<>();
        List<SearchItem> results = session.advanced().luceneQuery(SearchItem.class, "ContentSearchIndex")
            .waitForNonStaleResults()
            .highlight("Name", 20, 1, nameHighlightingRef)
            .search("Name", searchFor)
            .toList();

        assertTrue(nameHighlightingRef.value.getFragments("searchitems/1").length> 0);
        assertEquals("sample <b style=\"background:yellow\">about</b> a dog", nameHighlightingRef.value.getFragments("searchitems/1")[0]);
      }
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void highlightText_CantFindWork() throws Exception {
    SearchItem searchItem = new SearchItem();
    searchItem.setId("searchitems/1");
    searchItem.setName("This is a sample about a dog and his owner");
    String searchFor = "cat";


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(searchItem);
        store.getDatabaseCommands().putIndex(new ContentSearchIndex().getIndexName(), new ContentSearchIndex().createIndexDefinition());
        session.saveChanges();

        Reference<FieldHighlightings> nameHighlightingRef = new Reference<>();
        List<SearchItem> results = session.advanced().luceneQuery(SearchItem.class, "ContentSearchIndex")
            .waitForNonStaleResults()
            .highlight("Name", 20, 1, nameHighlightingRef)
            .search("Name", searchFor)
            .toList();

        assertTrue(nameHighlightingRef.value.getFragments("searchitems/1").length == 0);
      }
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void highlightText_FindAllReturences() throws Exception {
    SearchItem searchItem = new SearchItem();
    searchItem.setId("searchitems/1");
    searchItem.setName("Once there lived a dog. He was very greedy. There were many times that he had to pay for his greed. Each time the dog promised himself, “I have learnt my lesson. Now I will never be greedy again.” But he soon forgot his promises and was as greedy as ever." +
        "One afternoon, the dog was terribly hungry. He decided to go look for something to eat. Just outside his house, there was a bridge. “I will go and look for food on the other side of the bridge. The food there is definitely better,” he thought to himself." +
        "He walked across the wooden bridge and started sniffing around for food. Suddenly, he spotted a bone lying at a distance. “Ah, I am in luck. This looks a delicious bone,” he said." +
        "Without wasting any time, the hungry dog picked up the bone and was just about to eat it, when he thought, “Somebody might see here with this bone and then I will have to share it with them. So, I had better go home and eat it.” Holding the bone in his mouth, he ran towards his house." +
        "While crossing the wooden bridge, the dog looked down into the river. There he saw his own reflection. The foolish dog mistook it for another dog. “There is another dog in the water with bone in its mouth,” he thought. Greedy, as he was, he thought, “How nice it would be to snatch that piece of bone as well. Then, I will have two bones.”" +
        "So, the greedy dog looked at his reflection and growled. The reflection growled back, too. This made the dog angry. He looked down at his reflection and barked, “Woof! Woof!” As he opened his mouth, the bone in his mouth fell into the river. It was only when the water splashed that the greedy dog realized that what he had seen was nothing but his own reflections and not another dog. But it was too late. He had lost the piece of bone because of his greed. Now he had to go hungry.");
    String searchFor = "dog";


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(searchItem);
        store.getDatabaseCommands().putIndex(new ContentSearchIndex().getIndexName(), new ContentSearchIndex().createIndexDefinition());
        session.saveChanges();

        Reference<FieldHighlightings> nameHighlightingRef = new Reference<>();
        List<SearchItem> results = session.advanced().luceneQuery(SearchItem.class, "ContentSearchIndex")
            .waitForNonStaleResults()
            .highlight("Name", 128, 20, nameHighlightingRef)
            .search("Name", searchFor)
            .toList();
        String[] fragments = nameHighlightingRef.value.getFragments("searchitems/1");
        assertTrue(fragments.length > 0);
        int counter =  0;
        for (String fragment: fragments) {
          counter += StringUtils.countMatches(fragment, "<b style=\"background");
        }
        assertEquals(12, counter);
      }
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void highlightText_FindAllReturencesWithSeveralWords() throws Exception {
    SearchItem searchItem = new SearchItem();
    searchItem.setId("searchitems/1");
    searchItem.setName("Once there lived a dog. He was very greedy. There were many times that he had to pay for his greed. Each time the dog promised himself, “I have learnt my lesson. Now I will never be greedy again.” But he soon forgot his promises and was as greedy as ever." +
        "One afternoon, the dog was terribly hungry. He decided to go look for something to eat. Just outside his house, there was a bridge. “I will go and look for food on the other side of the bridge. The food there is definitely better,” he thought to himself." +
        "He walked across the wooden bridge and started sniffing around for food. Suddenly, he spotted a bone lying at a distance. “Ah, I am in luck. This looks a delicious bone,” he said." +
        "Without wasting any time, the hungry dog picked up the bone and was just about to eat it, when he thought, “Somebody might see here with this bone and then I will have to share it with them. So, I had better go home and eat it.” Holding the bone in his mouth, he ran towards his house." +
        "While crossing the wooden bridge, the dog looked down into the river. There he saw his own reflection. The foolish dog mistook it for another dog. “There is another dog in the water with bone in its mouth,” he thought. Greedy, as he was, he thought, “How nice it would be to snatch that piece of bone as well. Then, I will have two bones.”" +
        "So, the greedy dog looked at his reflection and growled. The reflection growled back, too. This made the dog angry. He looked down at his reflection and barked, “Woof! Woof!” As he opened his mouth, the bone in his mouth fell into the river. It was only when the water splashed that the greedy dog realized that what he had seen was nothing but his own reflections and not another dog. But it was too late. He had lost the piece of bone because of his greed. Now he had to go hungry.");
    String searchFor = "dog look";


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(searchItem);
        store.getDatabaseCommands().putIndex(new ContentSearchIndex().getIndexName(), new ContentSearchIndex().createIndexDefinition());
        session.saveChanges();

        Reference<FieldHighlightings> nameHighlightingRef = new Reference<>();
        List<SearchItem> results = session.advanced().luceneQuery(SearchItem.class, "ContentSearchIndex")
            .waitForNonStaleResults()
            .highlight("Name", 128, 20, nameHighlightingRef)
            .search("Name", searchFor)
            .toList();
        String[] fragments = nameHighlightingRef.value.getFragments("searchitems/1");
        assertTrue(fragments.length > 0);
        int counter =  0;
        for (String fragment: fragments) {
          counter += StringUtils.countMatches(fragment, "<b style=\"background");
        }
        assertEquals(14, counter);
      }
    }
  }



  public class ContentSearchIndex extends AbstractIndexCreationTask {
    public ContentSearchIndex() {
      QHighlightTest_SearchItem x = QHighlightTest_SearchItem.searchItem;
      map = "from doc in docs select new { doc.Name } ";
      index(x.name, FieldIndexing.ANALYZED);
      store(x.name, FieldStorage.YES);
      termVector(x.name, FieldTermVector.WITH_POSITIONS_AND_OFFSETS);
    }
  }

  @QueryEntity
  public static class SearchItem {
    private String name;
    private String id;
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }

  }

}
