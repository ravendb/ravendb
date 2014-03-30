package net.ravendb.client;

import static org.junit.Assert.fail;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

public class NameEnforcerTest {
  @Test
  public void testNames() {
    Reflections reflections = new Reflections("raven", new MethodAnnotationsScanner());
    Set<Method> annotatedWithTest = reflections.getMethodsAnnotatedWith(Test.class);
    List<String> classesWithIssues = new ArrayList<>();


    for (Method method: annotatedWithTest) {

      if (Character.isUpperCase(method.getName().charAt(0))) {
        fail("Method name should start with lower case!:" + method);
      }

      if (!method.getDeclaringClass().getName().endsWith("Test")) {
        classesWithIssues.add(method.getDeclaringClass().getName());
      }
    }
    if (!classesWithIssues.isEmpty()) {
      fail("Class contains @Test methods but does not end with Test:\n" + StringUtils.join(classesWithIssues, '\n'));
    }
  }
}
