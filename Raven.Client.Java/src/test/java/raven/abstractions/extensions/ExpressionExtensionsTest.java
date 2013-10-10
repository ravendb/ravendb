package raven.abstractions.extensions;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.samples.QDeveloper;

public class ExpressionExtensionsTest {

  @Test
  public void testToPropertyPath() {
    QDeveloper developer = QDeveloper.developer;
    assertEquals("Nick", ExpressionExtensions.toPropertyPath(developer.nick, '_'));
    assertEquals("MainSkill_Name", ExpressionExtensions.toPropertyPath(developer.mainSkill().name, '_'));
    assertEquals("Skills_,Name", ExpressionExtensions.toPropertyPath(developer.skills.get(0).name, '_'));
  }

}
