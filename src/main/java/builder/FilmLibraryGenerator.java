package builder;

import builder.DataGenerator.ConcreteGenerator;
import builder.DataGenerator.DataGenerator;
import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import model.Person;
import model.Title;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class FilmLibraryGenerator implements DatabaseGenerator {
  private MongoDatabase MFilmLibrary;
  private Random randGenerator = new Random();
  private DataGenerator dataGenerator;

  public FilmLibraryGenerator(final MongoClient mongoClient) {
    dataGenerator = new ConcreteGenerator();
    this.MFilmLibrary = mongoClient
            .getDatabase("MFilmLibrary");
    MFilmLibrary.getCollection("films");
    MFilmLibrary.getCollection("actors");
    MFilmLibrary.getCollection("agents");
  }

  public MongoDatabase generate() {
    return this.MFilmLibrary;
  }

  public DatabaseGenerator setDocuments() {
    insertActor();
    insertFilms();
    insertAgents();
    return this;
  }

  private void insertAgents() {
    Person[] actors = dataGenerator.getPersons();
    MongoCollection<Document> agentsCollection = MFilmLibrary.getCollection("agents");
    for (int i = 30; i < 40; i++) {
      Document tempDoc = new Document("first_name", actors[i].getFirst_name())
                                    .append("last_name", actors[i].getLast_name())
                                    .append("nationalities", getNationalities())
                                    .append("current_clients", getClients(10))
                                    .append("old_clients", getOldClients());
      if (i%2 == 0) {
        tempDoc.put("corporation", "corpo "+((Integer) randGenerator.nextInt(5)).toString());
      }
      agentsCollection.insertOne(tempDoc);
    }
  }

  private ArrayList<Document> getOldClients() {
    ArrayList<Document> documents = new ArrayList<Document>();
    ArrayList<String> names = getClients(20);
    final int size = names.size();
    for (int i = 0; i < size; i++) {
      documents.add(new Document("Old client", names.get(i))
                        .append("status", "No current contract"));
    }
    return documents;
  }

  private ArrayList<String> getClients(final int startFromIndex) {
    final int limit = randGenerator.nextInt(2)+2;
    ArrayList<String> result = new ArrayList<String>();
    Person[] actors = dataGenerator.getPersons();
    for (int i = 0; i < limit; i++) {
      result.add(actors[startFromIndex+i].getFirst_name()+" "
                  +actors[startFromIndex+i].getLast_name());
    }
    return result;
  }

  private void insertFilms() {
    Title[] films = dataGenerator.getTitles();
    MongoCollection<Document> filmsCollection = MFilmLibrary.getCollection("films");
    for (Title film : films) {
      filmsCollection.insertOne(new Document("title", film.getTitle())
                                    .append("release_date", dataGenerator.getDate())
                                    .append("film_director", getFilmDirector())
                                    .append("cast", getCast()));
    }

  }

  private ArrayList<Document> getCast() {
    ArrayList<Person> actors = new ArrayList<Person>(Arrays.asList(dataGenerator.getPersons()));
    ArrayList<Document> result = new ArrayList<Document>();
    final int limit = randGenerator.nextInt(7)+4;
    int rand = randGenerator.nextInt(40);
    for (int i = 0; i < limit; i++) {
      result.add(new Document("actor_name", actors.get(rand).getFirst_name())
                        .append("actor_last_name", actors.get(rand).getLast_name())
                        .append("role_in_film", actors
                                .get(randGenerator.nextInt(40)).getFirst_name()));
      actors.remove(rand);
      rand = randGenerator.nextInt(40);
    }
    return result;
  }

  private String getFilmDirector() {
    return randGenerator.nextBoolean() ? "Tom Jones" : "Mark Knopfler";
  }

  private void insertActor() {
    Person[] actors = dataGenerator.getPersons();
    MongoCollection<Document> actorsCollection = MFilmLibrary.getCollection("actors");
    for (Person actor : actors) {
      actorsCollection.insertOne(new Document("first_name", actor.getFirst_name())
              .append("last_name", actor.getLast_name())
              .append("date_of_birth", actor.getDateOfBirth())
              .append("growth", actor.getGrowth())
              .append("typical_role", getTypicalRoles())
              .append("nationality", getNationalities()));
    }
    insertDataForQuery();
  }

  private ArrayList<Document> getNationalities() {
    String [] c = {"Poland", "Spain", "Portugal", "Russia", "USA", "Australia"};
    String [] u = {"Alaska", "Arizona", "California", "Alabama"};
    String [] r = {"Altai", "Adygea", "Komi", "Kursk"};
    ArrayList<String> countries = new ArrayList<String>();
    countries.addAll(Arrays.asList(c));
    ArrayList<String> usaStates = new ArrayList<String>();
    usaStates.addAll(Arrays.asList(u));
    ArrayList<String> rusStates = new ArrayList<String>();
    rusStates.addAll(Arrays.asList(r));
    ArrayList<Document> result = new ArrayList<Document>();
    final int limit = randGenerator.nextInt(2) + 1;  //how many nationalities 1 xor 2;
    for (int i = 0; i < limit; i++) {
      String country = countries.get(randGenerator.nextInt(countries.size()));
      if (country.equals("Russia")) {
        result.add(new Document("Country", country)
                        .append("State", rusStates.get(randGenerator.nextInt(4))));
      } else if (country.equals("USA")) {
        result.add(new Document("Country", country)
                        .append("State", usaStates.get(randGenerator.nextInt(4))));
      } else {
        result.add(new Document("Country", country));
      }
      countries.remove(country);
    }
    return result;
  }

  private ArrayList<String> getTypicalRoles() {
    List<String> roles = new ArrayList<String>();
    roles.add("Comedy");
    roles.add("Fantasy");
    roles.add("Criminal");
    roles.add("SciFi");
    ArrayList<String> result = new ArrayList<String>();
    final int firstPosition = randGenerator.nextInt(4);
    result.add(roles.get(firstPosition));
    roles.remove(firstPosition);
    result.add(roles.get(randGenerator.nextInt(3)));
    return result;
  }

  private void insertDataForQuery() {
    MongoCollection<Document> actorsCollection = MFilmLibrary.getCollection("actors");
    actorsCollection.insertOne(new Document("first_name", "Al")
                                      .append("nationality", getNationalities()));
    ArrayList<Document> nat = new ArrayList<Document>();
    nat.add(new Document("Country", "Australia"));
    actorsCollection.insertOne(new Document("first_name", "Hugh")
                                      .append("nationality", nat));

    ArrayList<Document> newAgents = new ArrayList<Document>();
    newAgents.add(new Document("first_name", "KEVIN")
                      .append("last_name", "VALU"));
    newAgents.add(new Document("first_name", "KEVIN")
                      .append("last_name", "HANK"));
    MFilmLibrary.getCollection("agents").insertMany(newAgents);
    ArrayList<Document> cast = new ArrayList<Document>();
    cast.add(new Document("actor_name", "Mark")
            .append("actor_last_name", "Maco")
            .append("role_in_film", "square"));
    cast.add(new Document("actor_name", "Tony")
            .append("actor_last_name", "Jake")
            .append("role_in_film", "rectangle"));
    MFilmLibrary.getCollection("films").insertOne(new Document("title", "CIRCLE")
                            .append("release_date", "2020")
                            .append("film_director", "Rafal Maserak")
                            .append("cast", cast));
  }
}
