package raven.samples;

import java.util.List;

import com.mysema.query.annotations.QueryEntity;
@QueryEntity
public class Company {
  private Long id;
  private String name;
  private List<Person> employees;
  /**
   * @return the id
   */
  public Long getId() {
    return id;
  }
  /**
   * @param id the id to set
   */
  public void setId(Long id) {
    this.id = id;
  }
  /**
   * @return the name
   */
  public String getName() {
    return name;
  }
  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }
  /**
   * @return the employees
   */
  public List<Person> getEmployees() {
    return employees;
  }
  /**
   * @param employees the employees to set
   */
  public void setEmployees(List<Person> employees) {
    this.employees = employees;
  }


}
