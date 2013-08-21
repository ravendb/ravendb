package raven.client.indexes;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import raven.linq.dsl.IndexExpression;
import raven.linq.dsl.expressions.AnonymousExpression;
import raven.samples.Post;
import raven.samples.QComment;
import raven.samples.QPost;

import com.mysema.query.annotations.QueryEntity;

public class AbstractIndexCreationTaskTest {

  private static class MyIndex extends AbstractIndexCreationTask {
    QAbstractIndexCreationTaskTest_Foo f = new QAbstractIndexCreationTaskTest_Foo("f");
    QAbstractIndexCreationTaskTest_Result r = new QAbstractIndexCreationTaskTest_Result("r");
    public MyIndex() {

      map = IndexExpression.from(Foo.class).select(
          new AnonymousExpression()
          .with(r.something, f.something)
          .with("_", createField("DynamicKey", f.longItem, false, false))
          .with("s", spatialGenerate("spatial", f.longItem, f.longItem))
          );
    }
  }

  @Test
  public void testCreateIndex() {
    assertEquals("docs.Foos.Select(f => new {Something = f.Something, _ = this.CreateField(\"DynamicKey\", f.LongItem, false, false), s = AbstractIndexCreationTask.SpatialGenerate(\"spatial\", (double?) f.LongItem, (double?) f.LongItem)})",
    new MyIndex().createIndexDefinition().getMap());
  }


  @QueryEntity
  public static class Result {
    private String something;
    private Long key;
    private String whatever;
    private Long longItem;
    private Long dynamicKey;
    public String getSomething() {
      return something;
    }
    public void setSomething(String something) {
      this.something = something;
    }
    public Long getKey() {
      return key;
    }
    public void setKey(Long key) {
      this.key = key;
    }
    public String getWhatever() {
      return whatever;
    }
    public void setWhatever(String whatever) {
      this.whatever = whatever;
    }
    public Long getLongItem() {
      return longItem;
    }
    public void setLongItem(Long longItem) {
      this.longItem = longItem;
    }
    public Long getDynamicKey() {
      return dynamicKey;
    }
    public void setDynamicKey(Long dynamicKey) {
      this.dynamicKey = dynamicKey;
    }


  }

  @QueryEntity
  public static class Foo {
    private String id;
    private String something;
    private Map<String, Bar> items;
    private long longItem;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getSomething() {
      return something;
    }
    public void setSomething(String something) {
      this.something = something;
    }
    public Map<String, Bar> getItems() {
      return items;
    }
    public void setItems(Map<String, Bar> items) {
      this.items = items;
    }
    public long getLongItem() {
      return longItem;
    }
    public void setLongItem(long longItem) {
      this.longItem = longItem;
    }
  }

  @QueryEntity
  public static class Bar {
    private String whatever;

    public String getWhatever() {
      return whatever;
    }

    public void setWhatever(String whatever) {
      this.whatever = whatever;
    }
  }

  private class RecurseIndex extends AbstractIndexCreationTask {
    public RecurseIndex() {
      QPost p = QPost.post;
      QComment c = QComment.comment;
      map = IndexExpression.from(Post.class).selectMany(recurse(p.comments), new AnonymousExpression().with("test", c.test));
    }
  }

  @Test
  public void testRecurse() {
    assertEquals("docs.Posts.SelectMany(post => this.Recurse(post, x => x.Comments), (post, comment) => new {test = comment.Test})",
        new RecurseIndex().createIndexDefinition().getMap());
  }

}
