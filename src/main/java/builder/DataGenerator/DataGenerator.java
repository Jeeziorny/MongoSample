package builder.DataGenerator;

import model.Person;
import model.Title;

public interface DataGenerator {
  Person[] getPersons();
  Title[] getTitles();
  String getDate();
}
