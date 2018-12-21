package model;

public class Person {
  private String first_name;
  private String last_name;
  private String dateOfBirth;
  private double growth;

  public Person(String f, String l, String d, double g) {
    this.first_name = f;
    this.last_name = l;
    this.dateOfBirth = d;
    this.growth = g;
  }

  public String getFirst_name() {
    return first_name;
  }

  public void setFirst_name(String first_name) {
    this.first_name = first_name;
  }

  public String getLast_name() {
    return last_name;
  }

  public void setLast_name(String last_name) {
    this.last_name = last_name;
  }

  public String getDateOfBirth() {
    return dateOfBirth;
  }

  public void setDateOfBirth(String dateOfBirth) {
    this.dateOfBirth = dateOfBirth;
  }

  public double getGrowth() {
    return growth;
  }

  public void setGrowth(double growth) {
    this.growth = growth;
  }
}
