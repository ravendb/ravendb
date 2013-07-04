package raven.linq.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import raven.linq.dsl.utils.PathUtils;
import raven.samples.QCompany;
import raven.samples.QPerson;

public class PathUtilsTest {

  @Test
  public void testTwoRoots() {
    QPerson personA = new QPerson("a");
    QCompany company = new QCompany("b");

    assertFalse(PathUtils.checkForPathWithSingleGetter(personA));
    assertTrue(PathUtils.checkForPathWithSingleGetter(personA.firstname));
    assertFalse(PathUtils.checkForPathWithSingleGetter(company.employees.any()));
  }
}
