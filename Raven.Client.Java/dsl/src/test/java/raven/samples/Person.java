package raven.samples;

import java.util.List;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Person {
  private String firstname;
  private String lastname;
  private int age;
  private int dogs;
  private List<Pet> pets;

  /**
   * @return the pets
   */
  public List<Pet> getPets() {
    return pets;
  }
  /**
   * @param pets the pets to set
   */
  public void setPets(List<Pet> pets) {
    this.pets = pets;
  }
  /**
   * @return the dogs
   */
  public int getDogs() {
    return dogs;
  }
  /**
   * @param dogs the dogs to set
   */
  public void setDogs(int dogs) {
    this.dogs = dogs;
  }
  /**
   * @return the firstname
   */
  public String getFirstname() {
    return firstname;
  }
  /**
   * @param firstname the firstname to set
   */
  public void setFirstname(String firstname) {
    this.firstname = firstname;
  }
  /**
   * @return the lastname
   */
  public String getLastname() {
    return lastname;
  }
  /**
   * @param lastname the lastname to set
   */
  public void setLastname(String lastname) {
    this.lastname = lastname;
  }
  /**
   * @return the age
   */
  public int getAge() {
    return age;
  }
  /**
   * @param age the age to set
   */
  public void setAge(int age) {
    this.age = age;
  }


}
