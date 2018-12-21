package builder.DataGenerator;

import com.google.gson.Gson;
import model.Person;
import model.Title;

import java.io.File;
import java.io.FileReader;
import java.util.Random;

public class ConcreteGenerator implements DataGenerator {
  private Random randGenerator = new Random();

  public Person[] getPersons() {
    try {
      Gson gson = new Gson();
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource("actorData.json").getFile());
      return gson.fromJson(new FileReader(file), Person[].class);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public Title[] getTitles() {
    try {
      Gson gson = new Gson();
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource("filmData.json").getFile());
      return gson.fromJson(new FileReader(file), Title[].class);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public String getDate() {
    final Integer result = randGenerator.nextInt(45)+1973;
    switch(randGenerator.nextInt(2)) {
      case 0:
        return result.toString();
      default:
        return (result+"-"+(randGenerator.nextInt(12)+1)+"-"+randGenerator.nextInt(27)+1);
    }
  }
}
