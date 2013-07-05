package raven.abstractions.extensions;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.samples.QDeveloper;

public class ExpressionExtensionsTest {

  @Test
  public void testToPropertyPath() {
    QDeveloper developer = QDeveloper.developer;
    assertEquals("nick", ExpressionExtensions.toPropertyPath(developer.nick, '_'));
    assertEquals("mainSkill_name", ExpressionExtensions.toPropertyPath(developer.mainSkill().name, '_'));
    assertEquals("skills_,name", ExpressionExtensions.toPropertyPath(developer.skills.get(0).name, '_'));
  }

}
