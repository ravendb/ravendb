package net.ravendb.abstractions.extensions;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.extensions.ExpressionExtensions;
import net.ravendb.samples.QDeveloper;

import org.junit.Test;


public class ExpressionExtensionsTest {

  @Test
  public void testToPropertyPath() {
    QDeveloper developer = QDeveloper.developer;
    assertEquals("Nick", ExpressionExtensions.toPropertyPath(developer.nick, '_'));
    assertEquals("MainSkill_Name", ExpressionExtensions.toPropertyPath(developer.mainSkill().name, '_'));
    assertEquals("Skills_,Name", ExpressionExtensions.toPropertyPath(developer.skills.get(0).name, '_'));
  }

}
